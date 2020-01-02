package morm

import (
	"strings"
	"testing"
	"time"
)

type teststruct1 struct {
	ID        uint64 `sqlx:"id"`
	Name      string `sqlx:"name"`
	Dummy     string
	CreatedAt *time.Time `sqlx:"created_at"`
	UpdatedAt *time.Time `sqlx:"updated_at"`
	DeletedAt *time.Time `sqlx:"deleted_at"`
}

type teststruct2 struct {
	ID        uint64     `sqlx:"id"`
	Name      string     `sqlx:"name"`
	CreatedAt *time.Time `sqlx:"created_at"`
	UpdatedAt *time.Time `sqlx:"updated_at"`
	DeletedAt *time.Time `sqlx:"deleted_at"`
}

func (t *teststruct1) TableName() string {
	return "teststruct1"
}

func (t *teststruct2) TableName() string {
	return "teststruct2"
}

func (t *teststruct1) Save() error {
	return morm.Save(t, nil)
}

func (t *teststruct1) Update() error {
	return morm.Update(t, nil)
}

func (t *teststruct1) SetID(id uint64) {
	t.ID = id
}

func (t *teststruct1) GetID() uint64 {
	return t.ID
}

func TestPrepareModelForScan(t *testing.T) {
	prepareModelForScan(&teststruct1{})

	_, ok := modelFields["teststruct1"]

	if !ok {
		t.Error("teststruct not found in modelfields")
		t.FailNow()
	}
	prepareModelForScan(&teststruct2{})
	result := morm.GetSQLFields("teststruct1", "teststruct2")
	temp := result
	//hard to get a forecast on result because morm.GetSQLFields produces an output based on a map so in a random order

	expectedResult := []string{`teststruct1.id "teststruct1.id"`,
		`teststruct1.name "teststruct1.name"`,
		`teststruct1.created_at "teststruct1.created_at"`,
		`teststruct1.updated_at "teststruct1.updated_at"`,
		`teststruct1.deleted_at "teststruct1.deleted_at"`,
		`teststruct2.id "teststruct2.id"`,
		`teststruct2.name "teststruct2.name"`,
		`teststruct2.created_at "teststruct2.created_at"`,
		`teststruct2.updated_at "teststruct2.updated_at"`,
		`teststruct2.deleted_at "teststruct2.deleted_at"`}
	for _, v := range expectedResult {
		count := strings.Count(temp, v)
		if count == 0 {
			t.Errorf("%s is not present in result of SQLFields. Got :\n%s\n", v, result)
		}
		if count > 1 {
			t.Errorf("%s is present multiple times in result of SQLFields. Got : \n%s\n", v, result)
		}
		temp = strings.Replace(temp, v, "", 1)
	}
	temp = strings.ReplaceAll(temp, "\n", "")
	temp = strings.ReplaceAll(temp, ",", "")
	if temp != "" {
		t.Errorf(" Additionnal unexpected content in result : ---%s---", temp)
	}
}

func TestGetMapValues(t *testing.T) {
	/*ti := time.Date(2019, time.October, 10, 9, 0, 0, 0, time.UTC)
	m := GetMapValues(&teststruct1{Name: `bo"b`, UpdatedAt: nil, DeletedAt: nil, ID: 23, CreatedAt: &ti})
	/*if (*m)["name"] != `"bo\\\"b"` ||
		(*m)["created_at"] != `"2019-10-10T09:00:00"` ||
		(*m)["deleted_at"] != "null" ||
		(*m)["id"] != "23" {
		t.Errorf("Error on GetMapValues. Was expecting :\n"+
			"map[created_at:\"2019-10-10T09:00:00\" deleted_at:null id:23 name:\"bo\\\"b\" updated_at:null]\n and got \n%v", *m)
	}
	*/
}
