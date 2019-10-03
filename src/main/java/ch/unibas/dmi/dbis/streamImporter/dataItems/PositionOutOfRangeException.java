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

/**
 * Thrown to indicate that the x or y coordinate of a position is not in [-180.0, 180.0).
 * The fact that the x and y coordinate have to be in [-180,180) is a restriction of MongoDB's 2d index: https://docs.mongodb.com/manual/tutorial/build-a-2d-index/
 */
public class PositionOutOfRangeException extends Exception {

    /**
     * PositionOutOfRangeException constructor.
     *
     * @param msg Message that explains the problem
     */
    public PositionOutOfRangeException(String msg) {
        super(msg);
    }
}
