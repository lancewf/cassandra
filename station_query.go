package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/lancewf/sqldatabase"
	"log"
)

type StationQuery struct {
	session *gocql.Session
}

func NewStationQuery(session *gocql.Session) *StationQuery{
	query := &StationQuery{session}

	return query
}

func (stationQuery *StationQuery) Close() {
	stationQuery.session.Close()
}

func (stationQuery *StationQuery) CreateStation(station sqldatabase.Station) sqldatabase.Station {

	if err := stationQuery.session.Query("INSERT INTO station(id, label, latitude, longitude) VALUES(?, ?, ?)").Exec(); err != nil {
		log.Fatal(err)
	}
}

func (stationQuery *StationQuery) GetAllStations() []sqldatabase.Station {

	var (
		id int
		label string
		latitude float64
		longitude float64
	)

	stations := []sqldatabase.Station{}
	var rows = stationQuery.session.Query("SELECT * FROM users").Iter()
	for rows.Scan(&id, &label, &latitude, &longitude) {
		s := sqldatabase.Station{id, label, latitude, longitude}

		stations = append(stations, s)
	}

	return stations
}