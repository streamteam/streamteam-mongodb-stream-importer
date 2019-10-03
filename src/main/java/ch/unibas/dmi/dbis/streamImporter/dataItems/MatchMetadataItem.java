/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamImporter.dataItems;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * Match metadata item.
 */
public class MatchMetadataItem {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(MatchMetadataItem.class);

    /**
     * Match identifier
     */
    private String matchId;

    /**
     * Generation timestamp (in ms) of the first data stream element of the match
     */
    private Long generationTimestampFirstDataStreamElementOfTheMatch;

    /**
     * Sport discipline
     */
    private String sport;

    /**
     * Field size (width, height)
     */
    private List<Double> fieldSize;

    /**
     * Date in ISO 8601 format
     */
    private String date;

    /**
     * Context/Competition
     */
    private String competition;

    /**
     * Venue
     */
    private String venue;

    /**
     * Identifier of the home team
     */
    private String homeTeamId;

    /**
     * Identifier of the away team
     */
    private String awayTeamId;

    /**
     * Identifiers of the players of the home team
     */
    private List<String> homePlayerIds;

    /**
     * Identifiers of the players of the away team
     */
    private List<String> awayPlayerIds;

    /**
     * Name of the home team
     */
    private String homeTeamName;

    /**
     * Name of the away team
     */
    private String awayTeamName;

    /**
     * Names of the players of the home team
     */
    private List<String> homePlayerNames;

    /**
     * Names of the players of the away team
     */
    private List<String> awayPlayerNames;

    /**
     * Path where the video of the match can be found
     */
    private String videoPath;

    /**
     * Video offset (in s) of the start of the match
     */
    private Long matchStartVideoOffset;

    /**
     * Color of the home team
     */
    private String homeTeamColor;

    /**
     * Color of the away team
     */
    private String awayTeamColor;


    /**
     * MatchMetadataItem constructor.
     *
     * @param matchMetadataStreamElement matchMetadata stream element
     * @throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException Thrown if an information could not be retrieved from the matchMetadata stream element
     */
    public MatchMetadataItem(MatchMetadataStreamElement matchMetadataStreamElement) throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException {
        this.matchId = matchMetadataStreamElement.getKey();

        this.generationTimestampFirstDataStreamElementOfTheMatch = matchMetadataStreamElement.getGenerationTimestampFirstDataStreamElementOfTheMatch();

        this.sport = matchMetadataStreamElement.getSport();

        this.fieldSize = new LinkedList<>();
        this.fieldSize.add(matchMetadataStreamElement.getFieldLength());
        this.fieldSize.add(matchMetadataStreamElement.getFieldWidth());

        long matchStartUnixTs = matchMetadataStreamElement.getMatchStartUnixTs();
        // https://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format-with-date-hour-and-minute/3914973
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);
        this.date = df.format(new Date(matchStartUnixTs));

        this.competition = matchMetadataStreamElement.getCompetition();

        this.venue = matchMetadataStreamElement.getVenue();

        String homeTeamId = null;
        String awayTeamId = null;
        String homeTeamName = null;
        String awayTeamName = null;
        String teamRenameMapString = matchMetadataStreamElement.getTeamRenameMap();
        String[] teamRenameMapStringParts = teamRenameMapString.split("%");
        for (String teamRenameMapStringPart : teamRenameMapStringParts) {
            String teamRenameMapStringPartInner = teamRenameMapStringPart.substring(1, teamRenameMapStringPart.length() - 1);
            String[] teamRenameMapStringPartInnerParts = teamRenameMapStringPartInner.split(":");
            if (teamRenameMapStringPartInnerParts[0].equals("home")) {
                homeTeamId = teamRenameMapStringPartInnerParts[1];
                homeTeamName = teamRenameMapStringPartInnerParts[2];
            } else if (teamRenameMapStringPartInnerParts[0].equals("away")) {
                awayTeamId = teamRenameMapStringPartInnerParts[1];
                awayTeamName = teamRenameMapStringPartInnerParts[2];
            }
        }
        this.homeTeamId = homeTeamId;
        this.awayTeamId = awayTeamId;
        this.homeTeamName = homeTeamName;
        this.awayTeamName = awayTeamName;

        this.homePlayerIds = new LinkedList<>();
        this.awayPlayerIds = new LinkedList<>();
        this.homePlayerNames = new LinkedList<>();
        this.awayPlayerNames = new LinkedList<>();
        String playerRenameMapString = matchMetadataStreamElement.getObjectRenameMap();
        String[] playerRenameMapStringParts = playerRenameMapString.split("%");
        for (String playerRenameMapStringPart : playerRenameMapStringParts) {
            String playerRenameMapStringPartInner = playerRenameMapStringPart.substring(1, playerRenameMapStringPart.length() - 1);
            String[] playerRenameMapStringPartInnerParts = playerRenameMapStringPartInner.split(":");
            if (!playerRenameMapStringPartInnerParts[1].equals("BALL")) {
                if (playerRenameMapStringPartInnerParts[1].startsWith(homeTeamId)) {
                    this.homePlayerIds.add(playerRenameMapStringPartInnerParts[1]);
                    this.homePlayerNames.add(playerRenameMapStringPartInnerParts[2]);
                } else if (playerRenameMapStringPartInnerParts[1].startsWith(awayTeamId)) {
                    this.awayPlayerIds.add(playerRenameMapStringPartInnerParts[1]);
                    this.awayPlayerNames.add(playerRenameMapStringPartInnerParts[2]);
                }
            }
        }

        this.videoPath = matchMetadataStreamElement.getVideoPath();

        this.matchStartVideoOffset = matchMetadataStreamElement.getMatchStartVideoOffset();

        String homeTeamColor = null;
        String awayTeamColor = null;
        String teamColorsString = matchMetadataStreamElement.getTeamColorMap();
        String[] teamColorsStringParts = teamColorsString.split("%");
        for (String teamColorsStringPart : teamColorsStringParts) {
            String teamColorsStringPartInner = teamColorsStringPart.substring(1, teamColorsStringPart.length() - 1);
            String[] teamColorsStringPartInnerParts = teamColorsStringPartInner.split(":");
            if (teamColorsStringPartInnerParts[0].equals(homeTeamId)) {
                homeTeamColor = teamColorsStringPartInnerParts[1];
            } else if (teamColorsStringPartInnerParts[0].equals(awayTeamId)) {
                awayTeamColor = teamColorsStringPartInnerParts[1];
            }
        }
        this.homeTeamColor = homeTeamColor;
        this.awayTeamColor = awayTeamColor;
    }

    /**
     * Generates a MongoDB document.
     *
     * @return MongoDB document
     */
    public Document toDocument() {
        Document document = new Document("matchId", this.matchId)
                .append("sport", this.sport)
                .append("fieldSize", this.fieldSize)
                .append("date", this.date)
                .append("competition", this.competition)
                .append("venue", this.venue)
                .append("homeTeamId", this.homeTeamId)
                .append("awayTeamId", this.awayTeamId)
                .append("homePlayerIds", this.homePlayerIds)
                .append("awayPlayerIds", this.awayPlayerIds)
                .append("homeTeamName", this.homeTeamName)
                .append("awayTeamName", this.awayTeamName)
                .append("homePlayerNames", this.homePlayerNames)
                .append("awayPlayerNames", this.awayPlayerNames)
                .append("videoPath", this.videoPath)
                .append("homeTeamColor", this.homeTeamColor)
                .append("awayTeamColor", this.awayTeamColor);
        return document;
    }

    /**
     * Returns the match identifier.
     *
     * @return Match identifier
     */
    public String getMatchId() {
        return this.matchId;
    }

    /**
     * Returns the video offset (in s) of the start of the match.
     *
     * @return Video offset (in s) of the start of the match
     */
    public long getMatchStartVideoOffset() {
        return this.matchStartVideoOffset;
    }

    /**
     * Returns the generation timestamp (in ms) of the first data stream element of the match.
     *
     * @return Generation timestamp (in ms) of the first data stream element of the match
     */
    public long getGenerationTimestampFirstDataStreamElementOfTheMatch() {
        return this.generationTimestampFirstDataStreamElementOfTheMatch;
    }
}
