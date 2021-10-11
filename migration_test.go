package gomigrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MigrationList_All(t *testing.T) {
	tests := []struct {
		name string
		migs []Migration
	}{
		{
			name: "empty list",
			migs: []Migration{},
		},
		{
			name: "populated list",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			assert.Equal(t, test.migs, migList.All())
		})
	}
}

func Test_MigrationList_GetLatestMigration(t *testing.T) {
	tests := []struct {
		name              string
		migs              []Migration
		expectedMigration Migration
		shouldBeFound     bool
	}{
		{
			name:          "empty list",
			migs:          []Migration{},
			shouldBeFound: false,
		},
		{
			name: "single migration",
			migs: []Migration{
				{
					Id: "only-mig",
				},
			},
			expectedMigration: Migration{
				Id: "only-mig",
			},
			shouldBeFound: true,
		},
		{
			name: "multiple migrations",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigration: Migration{
				Id: "mig-3",
			},
			shouldBeFound: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			mig, found := migList.GetLatestMigration()

			assert.Equal(t, test.shouldBeFound, found)

			if !test.shouldBeFound {
				assert.Nil(t, mig)
				return
			}

			assert.Equal(t, test.expectedMigration, *mig)
		})
	}
}

func Test_MigrationList_GetFirstMigration(t *testing.T) {
	tests := []struct {
		name              string
		migs              []Migration
		expectedMigration Migration
		shouldBeFound     bool
	}{
		{
			name:          "empty list",
			migs:          []Migration{},
			shouldBeFound: false,
		},
		{
			name: "single migration",
			migs: []Migration{
				{
					Id: "only-mig",
				},
			},
			expectedMigration: Migration{
				Id: "only-mig",
			},
			shouldBeFound: true,
		},
		{
			name: "multiple migrations",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigration: Migration{
				Id: "mig-1",
			},
			shouldBeFound: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			mig, found := migList.GetFirstMigration()

			assert.Equal(t, test.shouldBeFound, found)

			if !test.shouldBeFound {
				assert.Nil(t, mig)
				return
			}

			assert.Equal(t, test.expectedMigration, *mig)
		})
	}
}

func Test_MigrationList_findById(t *testing.T) {
	tests := []struct {
		name              string
		migs              []Migration
		searchId          string
		expectedMigration Migration
		expectedIndex     int
	}{
		{
			name:          "search in empty list",
			searchId:      "mig-1",
			migs:          []Migration{},
			expectedIndex: -1,
		},
		{
			name:     "search for existing migration",
			searchId: "mig-2",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigration: Migration{
				Id: "mig-2",
			},
			expectedIndex: 1,
		},
		{
			name:     "search for first migration",
			searchId: "mig-1",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigration: Migration{
				Id: "mig-1",
			},
			expectedIndex: 0,
		},
		{
			name:     "search for last migration",
			searchId: "mig-3",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigration: Migration{
				Id: "mig-3",
			},
			expectedIndex: 2,
		},
		{
			name:     "search for non-existent migration",
			searchId: "mig-6",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedIndex: -1,
		},
		{
			// This is here to highlight that this list doesn't enforce unique Id's
			// It highlights the behaviour in that scenario
			// the first matching the id (in asc index order) is returned
			// Elements could have the same id, making this awkward
			// Definitely an improvement to be made in the future
			name:     "list with duplicate ids",
			searchId: "mig-2",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id:   "mig-2",
					Name: "should-be-found",
				},
				{
					Id: "mig-3",
				},
				{
					Id:   "mig-2",
					Name: "should-not-be-found",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigration: Migration{
				Id:   "mig-2",
				Name: "should-be-found",
			},
			expectedIndex: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			mig, index := migList.findById(test.searchId)

			assert.Equal(t, test.expectedIndex, index)

			if test.expectedIndex == -1 {
				assert.Nil(t, mig)
				return
			}

			assert.Equal(t, test.expectedMigration, *mig)
		})
	}
}

func Test_MigrationList_MigrationsUpToId(t *testing.T) {
	tests := []struct {
		name               string
		upToId             string
		migs               []Migration
		expectedMigrations []Migration
		expectedErr        error
	}{
		{
			name:               "empty list",
			upToId:             "mig-1",
			migs:               []Migration{},
			expectedMigrations: []Migration{},
			expectedErr: ErrMigrationNotFound{
				Id: "mig-1",
			},
		},
		{
			name:   "up to first migration",
			upToId: "mig-1",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-1",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "up to middle migration",
			upToId: "mig-3",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "up to final migration",
			upToId: "mig-4",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "up to non-existent migration",
			upToId: "mig-7",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{},
			expectedErr: ErrMigrationNotFound{
				Id: "mig-7",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			migs, err := migList.MigrationsUpToId(test.upToId)

			assert.Equal(t, test.expectedErr, err)
			assert.Equal(t, test.expectedMigrations, migs)
		})
	}
}

func Test_MigrationList_MigrationsFromId(t *testing.T) {
	tests := []struct {
		name               string
		upToId             string
		migs               []Migration
		expectedMigrations []Migration
		expectedErr        error
	}{
		{
			name:               "empty list",
			upToId:             "mig-1",
			migs:               []Migration{},
			expectedMigrations: []Migration{},
			expectedErr: ErrMigrationNotFound{
				Id: "mig-1",
			},
		},
		{
			name:   "from first migration",
			upToId: "mig-1",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "from middle migration",
			upToId: "mig-3",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "from final migration",
			upToId: "mig-4",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{
				{
					Id: "mig-4",
				},
			},
			expectedErr: nil,
		},
		{
			name:   "up to non-existent migration",
			upToId: "mig-7",
			migs: []Migration{
				{
					Id: "mig-1",
				},
				{
					Id: "mig-2",
				},
				{
					Id: "mig-3",
				},
				{
					Id: "mig-4",
				},
			},
			expectedMigrations: []Migration{},
			expectedErr: ErrMigrationNotFound{
				Id: "mig-7",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			migList := MigrationList{
				migrations: test.migs,
			}

			migs, err := migList.MigrationsFromId(test.upToId)

			assert.Equal(t, test.expectedErr, err)
			assert.Equal(t, test.expectedMigrations, migs)
		})
	}
}
