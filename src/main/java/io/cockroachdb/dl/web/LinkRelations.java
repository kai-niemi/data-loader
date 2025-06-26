package io.cockroachdb.dl.web;

public abstract class LinkRelations {
    public static final String ACTUATORS_REL = "actuators";

    public static final String DOWNLOAD_INDEX_REL = "downloads";

    public static final String DOWNLOAD_REL = "download";

    public static final String TABLE_INDEX_REL = "tables";

    public static final String TABLE_SCHEMA_REL = "table";

    public static final String TABLE_FORM_REL = "table-form";

    // IANA standard link relations:
    // http://www.iana.org/assignments/link-relations/link-relations.xhtml

    public static final String CURIE_NAMESPACE = "dl";

    private LinkRelations() {
    }

}
