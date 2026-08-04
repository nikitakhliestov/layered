package service

// Ride is the service's own view of a ride row — no sql/http types.
type Ride struct {
	ID            string
	DriverID      string
	Processed     bool
	ResultPayload string
}

type Driver struct {
	ID       string
	Name     string
	IsActive bool
}
