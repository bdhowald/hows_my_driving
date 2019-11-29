from enum import Enum


class NYPDBoroughBureaus(Enum):
    MANHATTAN_SOUTH = [1, 5, 6, 7, 9, 10, 13, 14, 17, 18]
    MANHATTAN_NORTH = [19, 20, 22, 23, 24, 25, 26, 28, 30, 32, 33, 34]
    BRONX = [40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52]
    BROOKLYN_SOUTH = [60, 61, 62, 63, 66, 67, 68, 69, 70, 71, 72, 76, 78]
    BROOKLYN_NORTH = [73, 75, 77, 79, 81, 83, 84, 88, 90, 94]
    QUEENS_SOUTH = [100, 101, 102, 103, 105, 106, 107, 113]
    QUEENS_NORTH = [104, 108, 109, 110, 111, 112, 114, 115]
    STATEN_ISLAND = [120, 121, 122, 123]


class NYPDBoroughs(Enum):
    MANHATTAN = {
        NYPDBoroughBureaus.MANHATTAN_SOUTH.name: NYPDBoroughBureaus.MANHATTAN_SOUTH.value,
        NYPDBoroughBureaus.MANHATTAN_NORTH.name: NYPDBoroughBureaus.MANHATTAN_NORTH.value,
    }
    BRONX = {
        NYPDBoroughBureaus.BRONX.name: NYPDBoroughBureaus.BRONX.value,
    }
    BROOKLYN = {
        NYPDBoroughBureaus.BROOKLYN_SOUTH.name: NYPDBoroughBureaus.BROOKLYN_SOUTH.value,
        NYPDBoroughBureaus.BROOKLYN_SOUTH.name: NYPDBoroughBureaus.BROOKLYN_SOUTH.value,
    }
    QUEENS = {
        NYPDBoroughBureaus.QUEENS_SOUTH.name: NYPDBoroughBureaus.QUEENS_SOUTH.value,
        NYPDBoroughBureaus.QUEENS_NORTH.name: NYPDBoroughBureaus.QUEENS_NORTH.value,
    }
    STATEN_ISLAND = {
        NYPDBoroughBureaus.STATEN_ISLAND.name: NYPDBoroughBureaus.STATEN_ISLAND.value,
    }

PRECINCTS_BY_BOROUGH = {borough.name: [precinct for grouping in [precinct_list for bureau, precinct_list in borough.value.items(
    )] for precinct in grouping] for borough in NYPDBoroughs}