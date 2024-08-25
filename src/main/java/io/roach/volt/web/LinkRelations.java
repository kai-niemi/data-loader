package io.roach.volt.web;

public abstract class LinkRelations {
    public static final String ACTUATORS_REL = "actuators";

    public static final String IMPORT_FILES_REL = "import-files";

    public static final String IMPORT_FILE_REL = "import-file";

    public static final String IMPORT_SQL_REL = "import-sql";

    // IANA standard link relations:
    // http://www.iana.org/assignments/link-relations/link-relations.xhtml

    public static final String CURIE_NAMESPACE = "volt";

    private LinkRelations() {
    }

}
