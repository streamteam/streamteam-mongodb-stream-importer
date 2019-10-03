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
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.NonAtomicEventPhase;
import org.bson.Document;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Data item.
 * Can be a tracking data item, an atomic event data item, a non-atomic event data item, or a statistics data item.
 */
public class DataItem {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DataItem.class);

    /**
     * Type of the data item (stream name of the data stream element)
     */
    private final String type;

    /**
     * Identifier of the match the data item belongs to
     */
    private final String matchId;

    /**
     * Time in ms since the start of the match
     */
    private final Integer ts;

    /**
     * Video offset (in s)
     */
    private final Integer videoTs;

    /**
     * Array containing the planar position(s) (x and y coordinates of the positions tuple of the data stream element)
     */
    private final List<List<Double>> xyCoords;

    /**
     * Array containing the z coordinates of the position(s) (z coordinates of the positions tuple of the data stream element)
     */
    private final List<Double> zCoords;

    /**
     * Array containing the involved players (object identifiers tuple of the data stream element)
     */
    private final List<String> playerIds;

    /**
     * Array containing the involved teams (group identifiers tuple of the data stream element)
     */
    private final List<String> teamIds;

    /**
     * Additional information (payload fields of the data stream element)
     */
    private final Document additionalInfo;

    /**
     * Identifier of the event the data item belongs to (only for non-atomic events)
     */
    private final String eventId;

    /**
     * Phase (only for non-atomic events)
     */
    private final NonAtomicEventPhase phase;

    /**
     * Sequence number (only for non-atomic events)
     */
    private final Long seqNo;

    /**
     * DataItem constructor.
     *
     * @param dataStreamElement                         Data stream element
     * @param generationTimestampFirstDataStreamElement Generation timestamp (in ms) of the first data stream element of the match
     * @param matchStartVideoOffset                     Video offset (in s) of the start of the match
     * @throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException Thrown if an information could not be retrieved from the data stream element
     * @throws PositionOutOfRangeException                                           Thrown if a position is not in interval [-180.0, 180.0).
     */
    public DataItem(AbstractImmutableDataStreamElement dataStreamElement, long generationTimestampFirstDataStreamElement, long matchStartVideoOffset) throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException, PositionOutOfRangeException {
        this.matchId = dataStreamElement.getKey();

        this.type = dataStreamElement.getStreamName();

        this.ts = calculateTs(dataStreamElement.getGenerationTimestamp(), generationTimestampFirstDataStreamElement);
        this.videoTs = calculateVideoTs(dataStreamElement.getGenerationTimestamp(), generationTimestampFirstDataStreamElement, matchStartVideoOffset);

        this.playerIds = dataStreamElement.getObjectIdentifiersList();
        this.teamIds = dataStreamElement.getGroupIdentifiersList();

        this.xyCoords = new LinkedList<>();
        this.zCoords = new LinkedList<>();
        for (Geometry.Vector position : dataStreamElement.getPositionsList()) {
            if (position.x >= 180.0 || position.x < -180.0 || position.y >= 180.0 || position.y < -180.0) { // x and y in [-180,180) (restriction by MongoDB: https://docs.mongodb.com/manual/tutorial/build-a-2d-index/)
                throw new PositionOutOfRangeException("X or Y coordinate of " + position.toString() + " is not in interval [-180.0, 180.0).");
            } else {
                List<Double> xyCoord = new LinkedList<>();
                xyCoord.add(position.x);
                xyCoord.add(position.y);
                this.xyCoords.add(xyCoord);
                this.zCoords.add(position.z);
            }
        }

        this.additionalInfo = new Document();
        for (Pair<String, Serializable> field : dataStreamElement.getPayloadFieldsAsKeyValueList()) {
            String key = field.getValue0();
            Serializable value = field.getValue1();
            this.additionalInfo.append(key, value);
        }

        if (dataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.EVENT) && !dataStreamElement.isAtomic()) {
            this.eventId = dataStreamElement.getEventIdentifier();
            this.phase = dataStreamElement.getPhase();
            this.seqNo = dataStreamElement.getSequenceNumber();
        } else {
            this.eventId = null;
            this.phase = null;
            this.seqNo = null;
        }
    }

    /**
     * Generates a MongoDB document.
     *
     * @return MongoDB document
     */
    public final Document toDocument() {
        Document document = new Document("type", this.type)
                .append("matchId", this.matchId)
                .append("ts", this.ts)
                .append("videoTs", this.videoTs)
                .append("xyCoords", this.xyCoords)
                .append("zCoords", this.zCoords)
                .append("playerIds", this.playerIds)
                .append("teamIds", this.teamIds)
                .append("additionalInfo", this.additionalInfo);

        if (this.eventId != null && this.phase != null && this.seqNo != null) {
            document.append("eventId", this.eventId)
                    .append("phase", this.phase.toString())
                    .append("seqNo", this.seqNo);
        }

        return document;
    }

    /**
     * Calculates the ts for the data item.
     * ts = Milliseconds since the start of the match
     *
     * @param generationTimestamp                       Generation timestamp from data stream element
     * @param generationTimestampFirstDataStreamElement Generation timestamp from the first data stream element of the match
     * @return Timestamp for the data item
     */
    private static int calculateTs(long generationTimestamp, long generationTimestampFirstDataStreamElement) {
        return (int) (generationTimestamp - generationTimestampFirstDataStreamElement);
    }

    /**
     * Calculates the videoTs for the data item.
     * videoTS = Video offset in seconds
     *
     * @param generationTimestamp                       Generation timestamp from data stream element
     * @param generationTimestampFirstDataStreamElement Generation timestamp from the first data stream element of the match
     * @param matchStartVideoOffset                     Offset in seconds for the start of the match
     * @return VideoTS for the data item
     */
    private static int calculateVideoTs(long generationTimestamp, long generationTimestampFirstDataStreamElement, long matchStartVideoOffset) {
        return (int) (matchStartVideoOffset + ((generationTimestamp - generationTimestampFirstDataStreamElement) / 1000));
    }
}
