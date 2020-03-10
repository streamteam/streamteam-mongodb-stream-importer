#!/bin/bash

#
# StreamTeam
# Copyright (C) 2019  University of Basel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#http://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

echo "Drop sportsense database from MongoDB"
mongo sportsense --eval "db.dropDatabase()"

# https://docs.mongodb.com/v3.6/core/schema-validation/ & https://docs.mongodb.com/v3.6/reference/operator/query/jsonSchema/#op._S_jsonSchema & https://docs.mongodb.com/v3.6/reference/operator/query/type/#document-type-available-types & https://stackoverflow.com/questions/44318188/add-new-validator-to-existing-collection
# Attention: The MongoDB JSON schema does not completely match the JSON Schema standard (see https://json-schema.org/learn/getting-started-step-by-step.html)
echo "Create collections"
mongo sportsense --eval 'db.createCollection("matches", {
  validator: {
    $jsonSchema: {
      properties: {
        matchId: {
          description: "Match identifier",
          bsonType: "string"
        },
        sport: {
          description: "Sport discipline",
          bsonType: "string"
        },
        fieldSize: {
          description: "Field size (width, height)",
          bsonType: "array",
          items: {
            bsonType: "double"
          }
        },
        date: {
          description: "Date in ISO 8601 format",
          bsonType: "string"
        },
        competition: {
          description: "Context/Competition",
          bsonType: "string"
        },
        venue: {
          description: "Venue",
          bsonType: "string"
        },
        homeTeamId: {
          description: "Identifier of the home team",
          bsonType: "string"
        },
        awayTeamId: {
          description: "Identifier of the away team",
          bsonType: "string"
        },
        homePlayerIds: {
          description: "Identifiers of the players of the home team",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        awayPlayerIds: {
          description: "Identifiers of the players of the away team",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        homeTeamName: {
          description: "Name of the home team",
          bsonType: "string"
        },
        awayTeamName: {
          description: "Name of the away team",
          bsonType: "string"
        },
        homePlayerNames: {
          description: "Names of the players of the home team",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        awayPlayerNames: {
          description: "Names of the players of the away team",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        videoPath: {
          description: "Path where the video of the match can be found",
          bsonType: "string"
        },
        homeTeamColor: {
          description: "Color of the home team",
          bsonType: "string"
        },
        awayTeamColor: {
          description: "Color of the away team",
          bsonType: "string"
        }
      },
      required: ["matchId", "sport", "fieldSize", "date", "competition", "venue", "homeTeamId", "awayTeamId", "homePlayerIds", "awayPlayerIds", "homeTeamName", "awayTeamName", "homePlayerNames", "awayPlayerNames", "videoPath", "homeTeamColor", "awayTeamColor"]
    }
  }
});'
mongo sportsense --eval 'db.createCollection("events", {
  validator: {
    $jsonSchema: {
      properties: {
        type: {
          description: "Type of the data item (stream name of the data stream element)",
          bsonType: "string"
        },
        matchId: {
          description: "Identifier of the match the data item belongs to",
          bsonType: "string"
        },
        ts: {
          description: "Time in ms since the start of the match",
          bsonType: "int"
        },
        videoTs: {
          description: "Video offset (in s)",
          bsonType: "int"
        },
        xyCoords: {
          description: "Array containing the planar position(s) (x and y coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "array",
            items: {
              bsonType: "double"
            }
          }
        },
        zCoords: {
          description: "Array containing the z coordinates of the position(s) (z coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "double"
          }
        },
        playerIds: {
          description: "Array containing the involved players (object identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        teamIds: {
          description: "Array containing the involved teams (group identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        additionalInfo: {
          description: "Additional information (payload fields of the data stream element)",
          bsonType: "object"
        }
      },
      required: ["type", "matchId", "ts", "videoTs", "xyCoords", "zCoords", "playerIds", "teamIds", "additionalInfo"]
    }
  }
});'
mongo sportsense --eval 'db.createCollection("statistics", {
  validator: {
    $jsonSchema: {
      properties: {
        type: {
          description: "Type of the data item (stream name of the data stream element)",
          bsonType: "string"
        },
        matchId: {
          description: "Identifier of the match the data item belongs to",
          bsonType: "string"
        },
        ts: {
          description: "Time in ms since the start of the match",
          bsonType: "int"
        },
        videoTs: {
          description: "Video offset (in s)",
          bsonType: "int"
        },
        xyCoords: {
          description: "Array containing the planar position(s) (x and y coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "array",
            items: {
              bsonType: "double"
            }
          }
        },
        zCoords: {
          description: "Array containing the z coordinates of the position(s) (z coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "double"
          }
        },
        playerIds: {
          description: "Array containing the involved players (object identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        teamIds: {
          description: "Array containing the involved teams (group identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        additionalInfo: {
          description: "Additional information (payload fields of the data stream element)",
          bsonType: "object"
        }
      },
      required: ["type", "matchId", "ts", "videoTs", "xyCoords", "zCoords", "playerIds", "teamIds", "additionalInfo"]
    }
  }
});'
mongo sportsense --eval 'db.createCollection("states", {
  validator: {
    $jsonSchema: {
      properties: {
        type: {
          description: "Type of the data item (stream name of the data stream element)",
          bsonType: "string"
        },
        matchId: {
          description: "Identifier of the match the data item belongs to",
          bsonType: "string"
        },
        ts: {
          description: "Time in ms since the start of the match",
          bsonType: "int"
        },
        videoTs: {
          description: "Video offset (in s)",
          bsonType: "int"
        },
        xyCoords: {
          description: "Array containing the planar position(s) (x and y coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "array",
            items: {
              bsonType: "double"
            }
          }
        },
        zCoords: {
          description: "Array containing the z coordinates of the position(s) (z coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "double"
          }
        },
        playerIds: {
          description: "Array containing the involved players (object identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        teamIds: {
          description: "Array containing the involved teams (group identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        additionalInfo: {
          description: "Additional information (payload fields of the data stream element)",
          bsonType: "object"
        }
      },
      required: ["type", "matchId", "ts", "videoTs", "xyCoords", "zCoords", "playerIds", "teamIds", "additionalInfo"]
    }
  }
});'
mongo sportsense --eval 'db.createCollection("nonatomicEvents", {
  validator: {
    $jsonSchema: {
      properties: {
        type: {
          description: "Type of the data item (stream name of the data stream element)",
          bsonType: "string"
        },
        matchId: {
          description: "Identifier of the match the data item belongs to",
          bsonType: "string"
        },
        ts: {
          description: "Time in ms since the start of the match",
          bsonType: "int"
        },
        videoTs: {
          description: "Video offset (in s)",
          bsonType: "int"
        },
        xyCoords: {
          description: "Array containing the planar position(s) (x and y coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "array",
            items: {
              bsonType: "double"
            }
          }
        },
        zCoords: {
          description: "Array containing the z coordinates of the position(s) (z coordinates of the positions tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "double"
          }
        },
        playerIds: {
          description: "Array containing the involved players (object identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        teamIds: {
          description: "Array containing the involved teams (group identifiers tuple of the data stream element)",
          bsonType: "array",
          items: {
            bsonType: "string"
          }
        },
        additionalInfo: {
          description: "Additional information (payload fields of the data stream element)",
          bsonType: "object"
        },
        eventId: {
          description: "Identifier of the event the data item belongs to",
          bsonType: "string"
        },
        phase: {
          description: "Phase",
          bsonType: "string"
        },
        seqNo: {
          description: "Sequence number",
          bsonType: "long"
        }
      },
      required: ["type", "matchId", "ts", "videoTs", "xyCoords", "zCoords", "playerIds", "teamIds", "additionalInfo", "eventId", "phase", "seqNo"]
    }
  }
});'

# https://docs.mongodb.com/v3.6/indexes/ & https://stackoverflow.com/questions/28666570/building-multiple-indexes-at-once
echo "Create indices"
mongo sportsense --eval 'db.matches.createIndexes( [ {matchId: 1}, {sport: 1}, {fieldSize: 1}, {date: 1}, {competition: 1}, {venue: 1}, {homeTeamId: 1}, {awayTeamId: 1}, {homePlayerIds: 1}, {awayPlayerIds: 1}, {homeTeamName: 1}, {awayTeamName: 1}, {homePlayerNames: 1}, {awayPlayerNames: 1}, {videoPath: 1}, {homeTeamColor: 1}, {awayTeamColor: 1} ]);'
mongo sportsense --eval 'db.events.createIndexes( [ {type: 1}, {matchId: 1}, {ts: 1}, {videoTs: 1}, {xyCoords: "2d"}, {zCoords: 1}, {playerIds: 1}, {teamIds: 1} ]);'
mongo sportsense --eval 'db.statistics.createIndexes( [ {type: 1}, {matchId: 1}, {ts: 1}, {videoTs: 1}, {xyCoords: "2d"}, {zCoords: 1}, {playerIds: 1}, {teamIds: 1} ]);'
mongo sportsense --eval 'db.states.createIndexes( [ {type: 1}, {matchId: 1}, {ts: 1}, {videoTs: 1}, {xyCoords: "2d"}, {zCoords: 1}, {playerIds: 1}, {teamIds: 1} ]);'
mongo sportsense --eval 'db.nonatomicEvents.createIndexes( [ {type: 1}, {matchId: 1}, {ts: 1}, {videoTs: 1}, {xyCoords: "2d"}, {zCoords: 1}, {playerIds: 1}, {teamIds: 1}, {eventId: 1}, {phase: 1}, {seqNo: 1} ]);'

#https://mobilemonitoringsolutions.com/memory-errors-mongodb-resolve/
echo "Increase MongoDB sort memory from 32MB to 256MB"
mongo 10.34.58.65/admin --eval 'db.runCommand( { setParameter : 1, "internalQueryExecMaxBlockingSortBytes" : 268435456 } )'

