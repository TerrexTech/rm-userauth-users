package model

// AggregateID for User aggregate.
const AggregateID = 1

// User defines the User aggregate.
type User struct {
	UserID    string `bson:"userID,omitempty" json:"userID,omitempty"`
	Email     string `bson:"email,omitempty" json:"email,omitempty"`
	FirstName string `bson:"firstName,omitempty" json:"firstName,omitempty"`
	LastName  string `bson:"lastName,omitempty" json:"lastName,omitempty"`
	UserName  string `bson:"userName,omitempty" json:"userName,omitempty"`
	Password  string `bson:"password,omitempty" json:"password,omitempty"`
	Role      string `bson:"role,omitempty" json:"role,omitempty"`
}
