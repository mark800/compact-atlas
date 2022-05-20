/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.compactatlas.webapp.rest;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.compactatlas.intg.AtlasErrorCode;
import org.apache.compactatlas.intg.SortOrder;
import org.apache.compactatlas.common.annotation.Timed;
import org.apache.compactatlas.intg.bulkimport.BulkImportResponse;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.repository.glossary.GlossaryService;
import org.apache.compactatlas.repository.glossary.GlossaryTermUtils;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossary;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryCategory;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryTerm;
import org.apache.compactatlas.intg.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.compactatlas.intg.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.compactatlas.intg.model.instance.AtlasRelatedObjectId;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@RequestMapping("api/atlas/v2/glossary")
@RestController
public class GlossaryREST {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.GlossaryREST");

    private final GlossaryService glossaryService;

    @Autowired
    public GlossaryREST(GlossaryService glossaryService) {
        this.glossaryService = glossaryService;
    }

    /**
     * Retrieve all glossaries registered with Atlas
     *
     * @param limit  page size - by default there is no paging
     * @param offset offset for pagination purpose
     * @param sort   Sort order, ASC (default) or DESC
     * @return List of glossary entities fitting the above criteria
     * @throws AtlasBaseException
     * @HTTP 200 List of existing glossaries fitting the search criteria or empty list if nothing matches
     */
    @GetMapping("")
    @Timed
    public List<AtlasGlossary> getGlossaries(@RequestParam(defaultValue = "-1") final String limit,
                                             @RequestParam(defaultValue = "0") final String offset,
                                             @RequestParam(defaultValue = "ASC") final String sort) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaries()");
            }


            List<AtlasGlossary> ret = glossaryService.getGlossaries(Integer.parseInt(limit), Integer.parseInt(offset), toSortOrder(sort));
            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get a specific Glossary
     *
     * @param glossaryGuid unique glossary identifier
     * @return Glossary
     * @throws AtlasBaseException
     * @HTTP 200 If glossary with given guid exists
     * @HTTP 404 If glossary GUID is invalid
     */
    @GetMapping("/{glossaryGuid}")
    @Timed
    public AtlasGlossary getGlossary(@PathVariable("glossaryGuid") String glossaryGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossary(" + glossaryGuid + ")");
            }
            AtlasGlossary ret = glossaryService.getGlossary(glossaryGuid);

            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get a specific Glossary
     *
     * @param glossaryGuid unique glossary identifier
     * @return Glossary
     * @throws AtlasBaseException
     * @HTTP 200 If glossary exists for given GUID
     * @HTTP 404 If glossary GUID is invalid
     */
    @GetMapping("/{glossaryGuid}/detailed")
    @Timed
    public AtlasGlossary.AtlasGlossaryExtInfo getDetailedGlossary(@PathVariable("glossaryGuid") String glossaryGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getDetailedGlossary(" + glossaryGuid + ")");
            }
            AtlasGlossary.AtlasGlossaryExtInfo ret = glossaryService.getDetailedGlossary(glossaryGuid);

            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get specific glossary term
     *
     * @param termGuid unique identifier for glossary term
     * @return Glossary term
     * @throws AtlasBaseException
     * @HTTP 200 If glossary term exists for given GUID
     * @HTTP 404 If glossary term GUID is invalid
     */
    @GetMapping("/term/{termGuid}")
    @Timed
    public AtlasGlossaryTerm getGlossaryTerm(@PathVariable("termGuid") String termGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryTerm(" + termGuid + ")");
            }
            AtlasGlossaryTerm ret = glossaryService.getTerm(termGuid);
            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get specific glossary category
     *
     * @param categoryGuid unique identifier for glossary category
     * @return Glossary category
     * @throws AtlasBaseException
     * @HTTP 200 If glossary category exists for given GUID
     * @HTTP 404 If glossary category GUID is invalid
     */
    @GetMapping("/category/{categoryGuid}")
    @Timed
    public AtlasGlossaryCategory getGlossaryCategory(@PathVariable("categoryGuid") String categoryGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("categoryGuid", categoryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryCategory(" + categoryGuid + ")");
            }
            AtlasGlossaryCategory ret = glossaryService.getCategory(categoryGuid);

            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create a glossary
     *
     * @param atlasGlossary Glossary definition, terms & categories can be anchored to a glossary
     *                      using the anchor attribute when creating the Term/Category
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If glossary creation was successful
     * @HTTP 400 If Glossary definition has invalid or missing information
     * @HTTP 409 If Glossary definition already exists (duplicate qualifiedName)
     */
    @PostMapping("")
    @Timed
    public AtlasGlossary createGlossary(@RequestBody AtlasGlossary atlasGlossary) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.createGlossary()");
            }
            return glossaryService.createGlossary(atlasGlossary);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create a glossary term
     *
     * @param glossaryTerm Glossary term definition, a term must be anchored to a Glossary at the time of creation
     *                     optionally it can be categorized as well
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If glossary term creation was successful
     * @HTTP 400 If Glossary term definition has invalid or missing information
     * @HTTP 409 If Glossary term already exists (duplicate qualifiedName)
     */
    @PostMapping("/term")
    //@Timed
    public AtlasGlossaryTerm createGlossaryTerm(@RequestBody AtlasGlossaryTerm glossaryTerm) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.createGlossaryTerm()");
            }
            if (Objects.isNull(glossaryTerm.getAnchor())) {
                throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
            }
            return glossaryService.createTerm(glossaryTerm);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create glossary terms in bulk
     *
     * @param glossaryTerm glossary term definitions
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If Bulk glossary terms creation was successful
     * @HTTP 400 If any glossary term definition has invalid or missing information
     */
    @PostMapping("/terms")
    //@Timed
    public List<AtlasGlossaryTerm> createGlossaryTerms(@RequestBody List<AtlasGlossaryTerm> glossaryTerm) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.createGlossaryTerms()");
            }
            for (AtlasGlossaryTerm term : glossaryTerm) {
                if (Objects.isNull(term.getAnchor())) {
                    throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
                }
            }
            return glossaryService.createTerms(glossaryTerm);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create glossary category
     *
     * @param glossaryCategory glossary category definition, a category must be anchored to a Glossary when creating
     *                         Optionally, terms belonging to the category and the hierarchy can also be defined during creation
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If glossary category creation was successful
     * @HTTP 400 If Glossary category definition has invalid or missing information
     * @HTTP 409 If Glossary category already exists (duplicate qualifiedName)
     */
    @PostMapping("/category")
    //@Timed
    public AtlasGlossaryCategory createGlossaryCategory(@RequestBody AtlasGlossaryCategory glossaryCategory) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.createGlossaryCategory()");
            }
            if (Objects.isNull(glossaryCategory.getAnchor())) {
                throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
            }
            return glossaryService.createCategory(glossaryCategory);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create glossary category in bulk
     *
     * @param glossaryCategory glossary category definitions
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If BULK glossary category creation was successful
     * @HTTP 400 If ANY Glossary category definition has invalid or missing information
     */
    @PostMapping("/categories")
    //@Timed
    public List<AtlasGlossaryCategory> createGlossaryCategories(@RequestBody List<AtlasGlossaryCategory> glossaryCategory) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.createGlossaryCategories()");
            }
            for (AtlasGlossaryCategory category : glossaryCategory) {
                if (Objects.isNull(category.getAnchor())) {
                    throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
                }

            }
            return glossaryService.createCategories(glossaryCategory);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Update the given glossary
     *
     * @param glossaryGuid    unique identifier for glossary
     * @param updatedGlossary Updated glossary definition
     * @return Glossary
     * @throws AtlasBaseException
     * @HTTP 200 If glossary update was successful
     * @HTTP 404 If glossary guid in invalid
     * @HTTP 400 If Glossary definition has invalid or missing information
     */
    @PutMapping("/{glossaryGuid}")
    @Timed
    public AtlasGlossary updateGlossary(@PathVariable("glossaryGuid") String glossaryGuid, @RequestBody AtlasGlossary updatedGlossary) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.updateGlossary(" + glossaryGuid + ")");
            }
            updatedGlossary.setGuid(glossaryGuid);
            return glossaryService.updateGlossary(updatedGlossary);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Partially update the glossary
     *
     * @param glossaryGuid   unique identifier for glossary term
     * @param partialUpdates Map containing keys as attribute names and values as corresponding attribute values
     * @return Updated glossary
     * @throws AtlasBaseException
     * @HTTP 200 If glossary partial update was successful
     * @HTTP 404 If glossary guid in invalid
     * @HTTP 400 If partial update parameters are invalid
     */
    @PutMapping("/{glossaryGuid}/partial")
    @Timed
    public AtlasGlossary partialUpdateGlossary(@PathVariable("glossaryGuid") String glossaryGuid, @RequestBody Map<String, String> partialUpdates) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.partialUpdateGlossary()");
            }

            if (MapUtils.isEmpty(partialUpdates)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "PartialUpdates missing or empty");
            }

            AtlasGlossary glossary = glossaryService.getGlossary(glossaryGuid);
            for (Map.Entry<String, String> entry : partialUpdates.entrySet()) {
                try {
                    glossary.setAttribute(entry.getKey(), entry.getValue());
                } catch (IllegalArgumentException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR, entry.getKey(), "Glossary");
                }
            }
            return glossaryService.updateGlossary(glossary);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Update the given glossary term
     *
     * @param termGuid     unique identifier for glossary term
     * @param glossaryTerm updated glossary term
     * @return Updated glossary term
     * @throws AtlasBaseException
     * @HTTP 200 If glossary term update was successful
     * @HTTP 404 If glossary term guid in invalid
     * @HTTP 400 If Glossary temr definition has invalid or missing information
     */
    @PutMapping("/term/{termGuid}")
    @Timed
    public AtlasGlossaryTerm updateGlossaryTerm(@PathVariable("termGuid") String termGuid, @RequestBody AtlasGlossaryTerm glossaryTerm) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.updateGlossaryTerm()");
            }
            glossaryTerm.setGuid(termGuid);
            return glossaryService.updateTerm(glossaryTerm);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Partially update the glossary term
     *
     * @param termGuid       unique identifier for glossary term
     * @param partialUpdates Map containing keys as attribute names and values as corresponding attribute values
     * @return Updated glossary term
     * @throws AtlasBaseException
     * @HTTP 200 If glossary partial update was successful
     * @HTTP 404 If glossary term guid in invalid
     * @HTTP 400 If partial attributes are invalid
     */
    @PutMapping("/term/{termGuid}/partial")
    @Timed
    public AtlasGlossaryTerm partialUpdateGlossaryTerm(@PathVariable("termGuid") String termGuid, @RequestBody Map<String, String> partialUpdates) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.partialUpdateGlossaryTerm()");
            }

            if (MapUtils.isEmpty(partialUpdates)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "PartialUpdates missing or empty");
            }

            AtlasGlossaryTerm glossaryTerm = glossaryService.getTerm(termGuid);
            for (Map.Entry<String, String> entry : partialUpdates.entrySet()) {
                try {
                    glossaryTerm.setAttribute(entry.getKey(), entry.getValue());
                } catch (IllegalArgumentException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR, "Glossary Term", entry.getKey());
                }
            }
            return glossaryService.updateTerm(glossaryTerm);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Update the given glossary category
     *
     * @param categoryGuid     unique identifier for glossary category
     * @param glossaryCategory updated glossary category
     * @return glossary category
     * @throws AtlasBaseException
     * @HTTP 200 If glossary category partial update was successful
     * @HTTP 404 If glossary category guid in invalid
     * @HTTP 400 If Glossary category definition has invalid or missing information
     */
    @PutMapping("/category/{categoryGuid}")
    @Timed
    public AtlasGlossaryCategory updateGlossaryCategory(@PathVariable("categoryGuid") String categoryGuid, @RequestBody AtlasGlossaryCategory glossaryCategory) throws AtlasBaseException {
        Servlets.validateQueryParamLength("categoryGuid", categoryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.updateGlossaryCategory()");
            }
            glossaryCategory.setGuid(categoryGuid);
            return glossaryService.updateCategory(glossaryCategory);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Partially update the glossary category
     *
     * @param categoryGuid   unique identifier for glossary term
     * @param partialUpdates Map containing keys as attribute names and values as corresponding attribute values
     * @return Updated glossary category
     * @throws AtlasBaseException
     * @HTTP 200 If glossary category partial update was successful
     * @HTTP 404 If glossary category guid in invalid
     * @HTTP 400 If category attributes are invalid
     */
    @PutMapping("/category/{categoryGuid}/partial")
    @Timed
    public AtlasGlossaryCategory partialUpdateGlossaryCategory(@PathVariable String categoryGuid, @RequestBody Map<String, String> partialUpdates) throws AtlasBaseException {
        Servlets.validateQueryParamLength("categoryGuid", categoryGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.partialUpdateGlossaryCategory()");
            }

            if (MapUtils.isEmpty(partialUpdates)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "PartialUpdates missing or empty");
            }

            AtlasGlossaryCategory glossaryCategory = glossaryService.getCategory(categoryGuid);
            for (Map.Entry<String, String> entry : partialUpdates.entrySet()) {
                try {
                    glossaryCategory.setAttribute(entry.getKey(), entry.getValue());
                } catch (IllegalArgumentException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR, "Glossary Category", entry.getKey());
                }
            }
            return glossaryService.updateCategory(glossaryCategory);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a glossary
     *
     * @param glossaryGuid unique identifier for glossary
     * @throws AtlasBaseException
     * @HTTP 204 If glossary delete was successful
     * @HTTP 404 If glossary guid in invalid
     */
    @DeleteMapping("/{glossaryGuid}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteGlossary(@PathVariable String glossaryGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.deleteGlossary(" + glossaryGuid + ")");
            }
            glossaryService.deleteGlossary(glossaryGuid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a glossary term
     *
     * @param termGuid unique identifier for glossary term
     * @throws AtlasBaseException
     * @HTTP 204 If glossary term delete was successful
     * @HTTP 404 If glossary term guid in invalid
     */
    @DeleteMapping("/term/{termGuid}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteGlossaryTerm(@PathVariable String termGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.deleteGlossaryTerm(" + termGuid + ")");
            }
            glossaryService.deleteTerm(termGuid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a glossary category
     *
     * @param categoryGuid unique identifier for glossary category
     * @throws AtlasBaseException
     * @HTTP 204 If glossary category delete was successful
     * @HTTP 404 If glossary category guid in invalid
     */
    @DeleteMapping("/category/{categoryGuid}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteGlossaryCategory(@PathVariable String categoryGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("categoryGuid", categoryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.deleteGlossaryCategory(" + categoryGuid + ")");
            }
            glossaryService.deleteCategory(categoryGuid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get terms belonging to a specific glossary
     *
     * @param glossaryGuid unique identifier for glossary
     * @param limit        page size - by default there is no paging
     * @param offset       starting offset for loading terms
     * @param sort         ASC(default) or DESC
     * @return List of terms associated with the glossary
     * @throws AtlasBaseException
     * @HTTP 200 List of glossary terms for the given glossary or an empty list
     * @HTTP 404 If glossary guid in invalid
     */
    @GetMapping("/{glossaryGuid}/terms")
    @Timed
    public List<AtlasGlossaryTerm> getGlossaryTerms(@PathVariable("glossaryGuid") String glossaryGuid,
                                                    @DefaultValue("-1") @RequestParam("limit") String limit,
                                                    @DefaultValue("0") @RequestParam("offset") String offset,
                                                    @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryTerms(" + glossaryGuid + ")");
            }

            return glossaryService.getGlossaryTerms(glossaryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get term headers belonging to a specific glossary
     *
     * @param glossaryGuid unique identifier for glossary
     * @param limit        page size - by default there is no paging
     * @param offset       starting offset for loading terms
     * @param sort         ASC(default) or DESC
     * @return List of terms associated with the glossary
     * @throws AtlasBaseException
     * @HTTP 200 List of glossary terms for the given glossary or an empty list
     * @HTTP 404 If glossary guid in invalid
     */
    @GetMapping("/{glossaryGuid}/terms/headers")
    @Timed
    public List<AtlasRelatedTermHeader> getGlossaryTermHeaders(@PathVariable("glossaryGuid") String glossaryGuid,
                                                               @DefaultValue("-1") @RequestParam("limit") String limit,
                                                               @DefaultValue("0") @RequestParam("offset") String offset,
                                                               @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryTermHeaders(" + glossaryGuid + ")");
            }

            return glossaryService.getGlossaryTermsHeaders(glossaryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get the categories belonging to a specific glossary
     *
     * @param glossaryGuid unique identifier for glossary term
     * @param limit        page size - by default there is no paging
     * @param offset       offset for pagination purpose
     * @param sort         ASC (default) or DESC
     * @return List of associated categories
     * @throws AtlasBaseException
     * @HTTP 200 List of glossary categories for the given glossary or an empty list
     * @HTTP 404 If glossary guid in invalid
     */
    @GetMapping("/{glossaryGuid}/categories")
    @Timed
    public List<AtlasGlossaryCategory> getGlossaryCategories(@PathVariable("glossaryGuid") String glossaryGuid,
                                                             @DefaultValue("-1") @RequestParam("limit") String limit,
                                                             @DefaultValue("0") @RequestParam("offset") String offset,
                                                             @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryCategories(" + glossaryGuid + ")");
            }

            return glossaryService.getGlossaryCategories(glossaryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get the categories belonging to a specific glossary
     *
     * @param glossaryGuid unique identifier for glossary term
     * @param limit        page size - by default there is no paging
     * @param offset       offset for pagination purpose
     * @param sort         ASC (default) or DESC
     * @return List of associated categories
     * @throws AtlasBaseException
     * @HTTP 200 List of glossary categories for the given glossary or an empty list
     * @HTTP 404 If glossary guid in invalid
     */
    @GetMapping("/{glossaryGuid}/categories/headers")
    @Timed
    public List<AtlasRelatedCategoryHeader> getGlossaryCategoriesHeaders(@PathVariable("glossaryGuid") String glossaryGuid,
                                                                         @DefaultValue("-1") @RequestParam("limit") String limit,
                                                                         @DefaultValue("0") @RequestParam("offset") String offset,
                                                                         @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("glossaryGuid", glossaryGuid);
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getGlossaryCategoriesHeaders(" + glossaryGuid + ")");
            }

            return glossaryService.getGlossaryCategoriesHeaders(glossaryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get all terms associated with the specific category
     *
     * @param categoryGuid unique identifier for glossary category
     * @param limit        page size - by default there is no paging
     * @param offset       offset for pagination purpose
     * @param sort         ASC (default) or DESC
     * @return List of associated terms
     * @throws AtlasBaseException
     * @HTTP 200 List of terms for the given category or an empty list
     * @HTTP 404 If glossary category guid in invalid
     */
    @GetMapping("/category/{categoryGuid}/terms")
    @Timed
    public List<AtlasRelatedTermHeader> getCategoryTerms(@PathVariable String categoryGuid,
                                                         @DefaultValue("-1") @RequestParam("limit") String limit,
                                                         @DefaultValue("0") @RequestParam("offset") String offset,
                                                         @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("categoryGuid", categoryGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getCategoryTerms(" + categoryGuid + ")");
            }

            return glossaryService.getCategoryTerms(categoryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get all related terms for a specific term
     *
     * @param termGuid unique identifier for glossary term
     * @param limit    page size - by default there is no paging
     * @param offset   offset for pagination purpose
     * @param sort     ASC (default) or DESC
     * @return List of all related terms
     * @throws AtlasBaseException
     * @HTTP 200 List of related glossary terms for the given glossary or an empty list
     * @HTTP 404 If glossary term guid in invalid
     */
    @GetMapping("/terms/{termGuid}/related")
    @Timed
    public Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> getRelatedTerms(@PathVariable String termGuid,
                                                                                        @DefaultValue("-1") @RequestParam("limit") String limit,
                                                                                        @DefaultValue("0") @RequestParam("offset") String offset,
                                                                                        @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getRelatedTermsInfo(" + termGuid + ")");
            }

            return glossaryService.getRelatedTerms(termGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get all entity headers assigned with the specified term
     *
     * @param termGuid GUID of the term
     * @param limit    page size - by default there is no paging
     * @param offset   offset for pagination purpose
     * @param sort     ASC (default) or DESC
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 List of entity headers (if any) for the given glossary or an empty list
     * @HTTP 404 If glossary term guid in invalid
     */
    @GetMapping("/terms/{termGuid}/assignedEntities")
    @Timed
    public List<AtlasRelatedObjectId> getEntitiesAssignedWithTerm(@PathVariable("termGuid") String termGuid,
                                                                  @DefaultValue("-1") @RequestParam("limit") String limit,
                                                                  @DefaultValue("0") @RequestParam("offset") String offset,
                                                                  @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getEntitiesAssignedWithTerm(" + termGuid + ")");
            }

            return glossaryService.getAssignedEntities(termGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Assign the given term to the provided list of entity headers
     *
     * @param termGuid         Glossary term GUID
     * @param relatedObjectIds Related Entity IDs to which the term has to be associated
     * @throws AtlasBaseException
     * @HTTP 204 If the term assignment was successful
     * @HTTP 400 If ANY of the entity header is invalid
     * @HTTP 404 If glossary guid in invalid
     */
    @PostMapping("/terms/{termGuid}/assignedEntities")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void assignTermToEntities(@PathVariable("termGuid") String termGuid, @RequestBody List<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {
        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.assignTermToEntities(" + termGuid + ")");
            }

            glossaryService.assignTermToEntities(termGuid, relatedObjectIds);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Remove the term assignment for the given list of entity headers
     *
     * @param termGuid         Glossary term GUID
     * @param relatedObjectIds List of related entity IDs from which the term has to be dissociated
     * @throws AtlasBaseException
     * @HTTP 204 If glossary term dissociation was successful
     * @HTTP 400 If ANY of the entity header is invalid
     * @HTTP 404 If glossary term guid in invalid
     */
    @DeleteMapping("/terms/{termGuid}/assignedEntities")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeTermAssignmentFromEntities(@PathVariable("termGuid") String termGuid, @RequestBody List<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {
        removeTermFromGlossary(termGuid, relatedObjectIds);
    }


    /**
     * Remove the term assignment for the given list of entity headers
     *
     * @param termGuid         Glossary term GUID
     * @param relatedObjectIds List of related entity IDs from which the term has to be dissociated
     * @throws AtlasBaseException
     * @HTTP 204 If glossary term dissociation was successful
     * @HTTP 400 If ANY of the entity header is invalid
     * @HTTP 404 If glossary term guid in invalid
     */
    @PutMapping("/terms/{termGuid}/assignedEntities")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void disassociateTermAssignmentFromEntities(@PathVariable("termGuid") String termGuid, @RequestBody List<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {
        removeTermFromGlossary(termGuid, relatedObjectIds);
    }


    private void removeTermFromGlossary(String termGuid, List<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {

        Servlets.validateQueryParamLength("termGuid", termGuid);

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.removeTermFromGlossary(" + termGuid + ")");
            }

            glossaryService.removeTermFromEntities(termGuid, relatedObjectIds);
        } finally {
            AtlasPerfTracer.log(perf);
        }

    }

    /**
     * Get all related categories (parent and children)
     *
     * @param categoryGuid unique identifier for glossary category
     * @param limit        page size - by default there is no paging
     * @param offset       offset for pagination purpose
     * @param sort         ASC (default) or DESC
     * @return List of related categories
     * @throws AtlasBaseException
     */
    @GetMapping("/category/{categoryGuid}/related")
    @Timed
    public Map<String, List<AtlasRelatedCategoryHeader>> getRelatedCategories(@PathVariable("categoryGuid") String categoryGuid,
                                                                              @DefaultValue("-1") @RequestParam("limit") String limit,
                                                                              @DefaultValue("0") @RequestParam("offset") String offset,
                                                                              @DefaultValue("ASC") @RequestParam("sort") final String sort) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GlossaryREST.getRelatedCategories()");
            }

            return glossaryService.getRelatedCategories(categoryGuid, Integer.parseInt(offset), Integer.parseInt(limit), toSortOrder(sort));

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private SortOrder toSortOrder(final String sort) {
        SortOrder ret = SortOrder.ASCENDING;
        if (!"ASC".equals(sort)) {
            if ("DESC".equals(sort)) {
                ret = SortOrder.DESCENDING;
            }
        }
        return ret;
    }

    /**
     * Get sample template for uploading/creating bulk AtlasGlossaryTerm
     *
     * @return Template File
     * @HTTP 400 If the provided fileType is not supported
     */
    @GetMapping("/import/template")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public StreamingOutput produceTemplate() {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                outputStream.write(GlossaryTermUtils.getGlossaryTermHeaders().getBytes());
            }
        };
    }

    /**
     * Upload glossary file for creating AtlasGlossaryTerms in bulk
     *
     * @param inputStream InputStream of file
     * @param fileDetail  FormDataContentDisposition metadata of file
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If glossary term creation was successful
     * @HTTP 400 If Glossary term definition has invalid or missing information
     * @HTTP 409 If Glossary term already exists (duplicate qualifiedName)
     */
    @PostMapping("/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    //@Timed
    public BulkImportResponse importGlossaryData(@FormDataParam("file") InputStream inputStream,
                                                 @FormDataParam("file") FormDataContentDisposition fileDetail) throws AtlasBaseException {
        return glossaryService.importGlossaryData(inputStream, fileDetail.getFileName());
    }
}