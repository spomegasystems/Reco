/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.reco.domains.commons;

/**
 * @author Seth Adarkwa Kwakwa
 *         email : sethkwakwa@corenett.com, kwasett@gmail.com           
 * @Date   Mar 29, 2012
 */
public class PageAccess {

    private String path;
    private String pageName;
    private String pageId;
    private String parentId;
    private String alias;

    /**
     * 
     * @param path
     * @param pageName 
     */
    public PageAccess(String path, String pageName) {
        this.path = path;
        this.pageName = pageName;
    }

    /**
     * 
     * @param path
     * @param pageName
     * @param pageId 
     */
    public PageAccess(String path, String pageName, String pageId) {
        this.path = path;
        this.pageName = pageName;
        this.pageId = pageId;
    }

    public PageAccess(String path, String pageName, String pageId, String parent, String alias) {
        this.path = path;
        this.pageName = pageName;
        this.pageId = pageId;
        this.parentId = parent;
        this.alias = alias;
    }

    /**
     * 
     * @param path
     * @param pageName
     * @param pageId
     * @param parentId 
     */
    public PageAccess(String path, String pageName, String pageId, String parentId) {
        this.path = path;
        this.pageName = pageName;
        this.pageId = pageId;
        this.parentId = parentId;
    }

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return the pageName
     */
    public String getPageName() {
        return pageName;
    }

    /**
     * @param pageName the pageName to set
     */
    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    /**
     * @return the pageId
     */
    public String getPageId() {
        return pageId;
    }

    /**
     * @param pageId the pageId to set
     */
    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    /**
     * @return the parentId
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * @param parentId the parentId to set
     */
    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * @return the alias
     */
    public String getAlias() {
        return alias;
    }

    /**
     * @param alias the alias to set
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }
}
