package gcosid

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// TagKey marks a struct field as an id field — the Go equivalent of CosID's
// `@CosId` annotation:
//
//	type Order struct {
//	    ID   int64  `cosid:"order"`
//	    Name string
//	}
//
// The tag value is the generator name in the Injector's registry. `cosid:"-"`
// opts a field out, and an empty value (`cosid:""`) means the shared generator,
// which is what CosID's `@CosId` without a value does. String fields accept a
// StringGenerator; numeric fields accept a Generator.
const TagKey = "cosid"

// Injector fills id fields of structs from registered generators. CosID reaches
// the fields through reflection on annotated classes; Go does the same through
// struct tags, so an entity does not have to know about the generator at all.
type Injector struct {
	mu         sync.RWMutex
	generators map[string]Generator
	strings    map[string]StringGenerator
	provider   *Provider
	plans      sync.Map // reflect.Type -> []fieldPlan
}

// fieldPlan is one resolved id field.
type fieldPlan struct {
	index []int
	tag   string
	kind  reflect.Kind
}

// NewInjector creates an empty injector.
func NewInjector() *Injector {
	return &Injector{
		generators: map[string]Generator{},
		strings:    map[string]StringGenerator{},
	}
}

// NewInjectorFromProvider creates an injector backed by a Provider's named
// generators. The registry is read on every fill, so generators may be replaced
// at runtime.
func NewInjectorFromProvider(provider *Provider) *Injector {
	return &Injector{generators: nil, strings: nil, provider: provider}
}

// Register binds a generator name to an int64 id field.
func (i *Injector) Register(name string, gen Generator) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.generators == nil {
		i.generators = map[string]Generator{}
	}
	i.generators[name] = gen
}

// RegisterString binds a generator name to a string id field.
func (i *Injector) RegisterString(name string, gen StringGenerator) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.strings == nil {
		i.strings = map[string]StringGenerator{}
	}
	i.strings[name] = gen
}

// Ensure fills every empty id field of the entity. It reports whether anything
// was filled, mirroring CosID's EnsureId: an entity that already carries an id is
// left untouched.
func (i *Injector) Ensure(ctx context.Context, entity any) (bool, error) {
	plans, err := i.plan(entity)
	if err != nil {
		return false, err
	}

	value := reflect.ValueOf(entity).Elem()
	filled := false
	for _, plan := range plans {
		field := value.FieldByIndex(plan.index)
		if !field.CanSet() {
			return false, fmt.Errorf("%w: field %q of %s cannot be set", ErrInvalidConfig, plan.tag, value.Type())
		}
		if !isEmptyID(field) {
			continue
		}
		if err := i.fill(ctx, field, plan.tag); err != nil {
			return filled, err
		}
		filled = true
	}
	return filled, nil
}

// EnsureAll fills every entity of a slice or array.
func (i *Injector) EnsureAll(ctx context.Context, entities any) (int, error) {
	slice := reflect.ValueOf(entities)
	if slice.Kind() != reflect.Slice && slice.Kind() != reflect.Array {
		return 0, fmt.Errorf("%w: EnsureAll expects a slice, got %s", ErrInvalidConfig, slice.Kind())
	}

	filled := 0
	for idx := 0; idx < slice.Len(); idx++ {
		element := slice.Index(idx)
		if element.Kind() != reflect.Pointer {
			return filled, fmt.Errorf("%w: element %d is not a pointer", ErrInvalidConfig, idx)
		}
		ok, err := i.Ensure(ctx, element.Interface())
		if err != nil {
			return filled, err
		}
		if ok {
			filled++
		}
	}
	return filled, nil
}

// fieldGenerators returns the registries for one fill.
func (i *Injector) lookup(name string) (Generator, StringGenerator, error) {
	if name == "" {
		name = ShareName
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.provider != nil {
		gen, ok := i.provider.Get(name)
		if !ok {
			return nil, nil, fmt.Errorf("%w: no id generator named %q", ErrInvalidConfig, name)
		}
		if sg, isString := gen.(StringGenerator); isString {
			return gen, sg, nil
		}
		return gen, nil, nil
	}

	gen, hasGen := i.generators[name]
	sgen, hasString := i.strings[name]
	if !hasGen && !hasString {
		return nil, nil, fmt.Errorf("%w: no id generator named %q", ErrInvalidConfig, name)
	}
	return gen, sgen, nil
}

func (i *Injector) fill(ctx context.Context, field reflect.Value, name string) error {
	gen, stringGen, err := i.lookup(name)
	if err != nil {
		return err
	}

	if field.Kind() == reflect.String {
		if stringGen != nil {
			id, err := stringGen.GenerateAsString(ctx)
			if err != nil {
				return err
			}
			field.SetString(id)
			return nil
		}
		if gen == nil {
			return fmt.Errorf("%w: generator %q cannot produce strings", ErrInvalidConfig, name)
		}
		id, err := gen.Next(ctx)
		if err != nil {
			return err
		}
		field.SetString(ToString{}.Format(id))
		return nil
	}

	if gen == nil {
		return fmt.Errorf("%w: generator %q cannot produce numbers", ErrInvalidConfig, name)
	}
	id, err := gen.Next(ctx)
	if err != nil {
		return err
	}
	return setNumeric(field, name, id)
}

func setNumeric(field reflect.Value, name string, id int64) error {
	switch field.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32:
		if field.OverflowInt(id) {
			return fmt.Errorf("%w: id %d does not fit %s", ErrExhausted, id, field.Type())
		}
		field.SetInt(id)
		return nil
	case reflect.Uint64, reflect.Uint, reflect.Uint32:
		if id < 0 || field.OverflowUint(uint64(id)) {
			return fmt.Errorf("%w: id %d does not fit %s", ErrExhausted, id, field.Type())
		}
		field.SetUint(uint64(id))
		return nil
	default:
		return fmt.Errorf("%w: generator %q cannot fill a %s field", ErrInvalidConfig, name, field.Kind())
	}
}

func isEmptyID(field reflect.Value) bool {
	switch field.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32:
		return field.Int() == 0
	case reflect.Uint64, reflect.Uint, reflect.Uint32:
		return field.Uint() == 0
	case reflect.String:
		return field.String() == ""
	default:
		return false
	}
}

// plan resolves (and caches) the tagged fields of an entity's type, including
// fields of embedded structs so composite entities work like CosID's
// CompositeFieldDefinitionParser.
func (i *Injector) plan(entity any) ([]fieldPlan, error) {
	if entity == nil {
		return nil, fmt.Errorf("%w: entity is nil", ErrInvalidConfig)
	}

	rv := reflect.ValueOf(entity)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, fmt.Errorf("%w: entity must be a non-nil pointer so ids can be set", ErrInvalidConfig)
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: entity must point to a struct, got %s", ErrInvalidConfig, rv.Kind())
	}

	if cached, ok := i.plans.Load(rv.Type()); ok {
		return cached.([]fieldPlan), nil
	}

	plans, err := collectPlans(rv.Type(), nil)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, fmt.Errorf("%w: %s has no field tagged `%s:\"...\"`", ErrInvalidConfig, rv.Type(), TagKey)
	}
	i.plans.Store(rv.Type(), plans)
	return plans, nil
}

func collectPlans(structType reflect.Type, prefix []int) ([]fieldPlan, error) {
	var plans []fieldPlan
	for idx := 0; idx < structType.NumField(); idx++ {
		field := structType.Field(idx)
		index := append(append([]int{}, prefix...), idx)

		// Embedded structs are searched too, so an entity can inherit its id.
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			nested, err := collectPlans(field.Type, index)
			if err != nil {
				return nil, err
			}
			plans = append(plans, nested...)
			continue
		}

		tag, ok := field.Tag.Lookup(TagKey)
		// `cosid:"-"` opts out; an empty tag means the shared generator, like
		// `@CosId` with no value in CosID.
		if !ok || tag == "-" {
			continue
		}
		if !availableIDKind(field.Type.Kind()) {
			return nil, fmt.Errorf("%w: field %s.%s is %s, which cannot hold an id",
				ErrInvalidConfig, structType.Name(), field.Name, field.Type)
		}
		plans = append(plans, fieldPlan{index: index, tag: tag, kind: field.Type.Kind()})
	}
	return plans, nil
}

// availableIDKind is CosIdAccessor.availableType: string and the integer widths
// that can hold a snowflake or segment id.
func availableIDKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.String, reflect.Int64, reflect.Int, reflect.Int32, reflect.Uint64, reflect.Uint, reflect.Uint32:
		return true
	default:
		return false
	}
}
