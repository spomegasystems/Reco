/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.repo.util;

import org.neo4j.graphdb.Label;

/**
 *
 * @author Joseph George Davis
 * @date Jul 16, 2015 10:58:26 AM
 * @description-
 */
public enum Labels implements Label {
    AGENT,
    PARENT,
    FARMER,
    FARM_MANAGEMENT,
    FARM_OPERATION,
    HARVEST,
    MARKETING,
    POSTHARVEST,
    PRODUCTION,
    PRODUCTION_UPDATE,
    POSTHARVEST_UPATE,
    POSTHARVEST_BUDGET_UPDATE,
    PROFILE,
    STORAGE,
    TECHNICAL_NEEDS,
    FARM_MANAGEMENT_PLAN,
    USER,
    MEETING,
    MEETING_SETTING,
    MOBILE_TRACKER,
    MEETING_ACTIVITY,
    CROP_CALENDAR_SETTING,
    CROP_CALENDAR,
    FARM_INPUT,
    UPDATE,
    QUESTION,
    BASELINE_PRODUCTION,
    BASELINE_PRODUCTION_BUDGET,
    BASELINE_POST_HARVEST_BUDGET, 
    BASELINE_POST_HARVEST, 
    FMP_PRODUCTION_BUDGET, 
    FMP_PRODUCTION_BUDGET_UPDATE, 
    FMP_POSTHARVEST_BUDGET, 
    FIELD_CROP_ASSESSMENT,
    FARM_GPS,
    WEATHER,
    IMAGE,
    FARM_CREDIT_PLAN,
    FARM_CREDIT_PREVIOUS,
    FARM_CREDIT_UPDATE,
    PRODUCTION_BUDGET_UPDATE
    
}
