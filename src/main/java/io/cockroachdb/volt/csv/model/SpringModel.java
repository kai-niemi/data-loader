package io.cockroachdb.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SpringModel {
    @JsonProperty("datasource")
    private DataSource dataSource;

    public DataSource getDataSource() {
        return dataSource;
    }

    public SpringModel setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public static class DataSource {
        private String url;

        private String username;

        private String password;

        public String getPassword() {
            return password;
        }

        public DataSource setPassword(String password) {
            this.password = password;
            return this;
        }

        public String getUrl() {
            return url;
        }

        public DataSource setUrl(String url) {
            this.url = url;
            return this;
        }

        public String getUsername() {
            return username;
        }

        public DataSource setUsername(String username) {
            this.username = username;
            return this;
        }
    }
}
