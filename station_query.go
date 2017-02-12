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

	if err := stationQuery.session.Query("INSERT INTO station(id, label, latitude, longitude) VALUES(?, ?, ?)",
		station.Id, station.Label, station.Latitude, station.Longitude).Exec(); err != nil {
		log.Fatal(err)
	}

	return station
}

func (stationQuery *StationQuery) GetAllStations() []sqldatabase.Station {

	var (
		id int
		label string
		latitude float64
		longitude float64
	)

	stations := []sqldatabase.Station{}
	var rows = stationQuery.session.Query("select id, label, lat, lon from station3").Iter()
	for rows.Scan(&id, &label, &latitude, &longitude) {
		s := sqldatabase.NewStation(id, label, latitude, longitude)

		stations = append(stations, s)
	}

	return stations
}

func (stationQuery *StationQuery) CreateEnhancedParameter(enhancedParameter sqldatabase.EnhancedParameter) sqldatabase.EnhancedParameter {

	if err := stationQuery.session.Query("INSERT INTO enhancedparameter(id, parameter, cellmethods, interval, verticaldatum) VALUES(?, ?, ?)",
		enhancedParameter.Id, enhancedParameter.ParameterId, enhancedParameter.CellMethods, enhancedParameter.Interval, enhancedParameter.VerticalDatum).Exec(); err != nil {
		log.Fatal(err)
	}

	return enhancedParameter
}


func (stationQuery *StationQuery) GetAllEnhancedParameters() []sqldatabase.EnhancedParameter {

	var (
		id int
		parameterId int
		cellMethods string
		interval string
		verticaldatum string
	)

	enhancedParameterCollection := []sqldatabase.EnhancedParameter{}
	var rows = stationQuery.session.Query("select id, parameter, cellmethods, interval, verticaldatum from enhancedparameter").Iter()
	for rows.Scan(&id, &parameterId, &cellMethods, &interval, &verticaldatum) {
		ep := sqldatabase.NewEnhancedParameter(id, parameterId, cellMethods, interval, verticaldatum)

		enhancedParameterCollection = append(enhancedParameterCollection, ep)
	}

	return enhancedParameterCollection
}