package query

import (
	"encoding/json"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rm-userauth-users/model"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func loginUser(coll *mongo.Collection, query *cmodel.Command) ([]byte, *cmodel.Error) {
	user := &model.User{}
	err := json.Unmarshal(query.Data, user)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling query-data to user")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	result, err := coll.FindOne(model.User{
		UserName: user.UserName,
	})
	if err != nil {
		err = errors.Wrap(err, "Error getting user from database")
		return nil, cmodel.NewError(cmodel.UserError, err.Error())
	}

	loggedUser, assertOK := result.(*model.User)
	if !assertOK {
		err := errors.New("Error asserting loggedUser")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	err = bcrypt.CompareHashAndPassword([]byte(loggedUser.Password), []byte(user.Password))
	if err != nil {
		err = errors.Wrap(err, "Error comparing password")
		return nil, cmodel.NewError(cmodel.UserError, err.Error())
	}

	// Don't wanna send the password as result
	loggedUser.Password = ""
	marshalUser, err := json.Marshal(loggedUser)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling loggedUser")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	return marshalUser, nil
}
