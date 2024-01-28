package bus

type Storage interface {
	Load() ([]*Event, error)
	Save(*Event) error
}
