package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.eclipse.emf.common.util.EList;

import java.util.List;

@Builder
@Getter
@ToString
public class FilterJoinProcessorParameters {
    @NonNull
    private List<RdbmsJoin> joins;
    @NonNull
    private SubSelect query;
    @NonNull
    private List<Join> processedNodesForJoins;
    @NonNull
    private List<RdbmsField> conditions;
    @Builder.Default
    private String partnerTablePrefix = "";
    @NonNull
    private Filter filter;
    @NonNull
    private Node partnerTable;
    private boolean addJoinsOfFilterFeature;
}
