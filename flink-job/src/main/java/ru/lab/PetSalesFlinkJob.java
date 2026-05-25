package ru.lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PetSalesFlinkJob {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = env("KAFKA_TOPIC", "pet-sales");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("pet-sales-flink-lab")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "pet-sales-json")
                .name("Read pet sales JSON from Kafka")
                .addSink(new PostgresStarSink(
                        env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/lab3"),
                        env("POSTGRES_USER", "lab"),
                        env("POSTGRES_PASSWORD", "lab")))
                .name("Upsert PostgreSQL star schema");

        environment.execute("pet-sales-flink-streaming-etl");
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static class PostgresStarSink extends RichSinkFunction<String> {
        private static final DateTimeFormatter SOURCE_DATE = DateTimeFormatter.ofPattern("M/d/yyyy");
        private final String jdbcUrl;
        private final String user;
        private final String password;
        private transient ObjectMapper mapper;
        private transient Connection connection;
        private transient PreparedStatement countryStatement;
        private transient PreparedStatement postalAreaStatement;
        private transient PreparedStatement cityStatement;
        private transient PreparedStatement productCategoryStatement;
        private transient PreparedStatement petCategoryStatement;
        private transient PreparedStatement customerStatement;
        private transient PreparedStatement sellerStatement;
        private transient PreparedStatement supplierStatement;
        private transient PreparedStatement storeStatement;
        private transient PreparedStatement productStatement;
        private transient PreparedStatement dateStatement;
        private transient PreparedStatement factStatement;

        public PostgresStarSink(String jdbcUrl, String user, String password) {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapper = new ObjectMapper();
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setAutoCommit(false);
            countryStatement = connection.prepareStatement(
                    "INSERT INTO dim_country (country_key, country_name) VALUES (?, ?) "
                            + "ON CONFLICT (country_key) DO NOTHING");
            postalAreaStatement = connection.prepareStatement(
                    "INSERT INTO dim_postal_area (postal_area_key, country_key, postal_code) VALUES (?, ?, ?) "
                            + "ON CONFLICT (postal_area_key) DO NOTHING");
            cityStatement = connection.prepareStatement(
                    "INSERT INTO dim_city (city_key, city_name, state_name, country_key) VALUES (?, ?, ?, ?) "
                            + "ON CONFLICT (city_key) DO NOTHING");
            productCategoryStatement = connection.prepareStatement(
                    "INSERT INTO dim_product_category (product_category_key, product_category_name) VALUES (?, ?) "
                            + "ON CONFLICT (product_category_key) DO NOTHING");
            petCategoryStatement = connection.prepareStatement(
                    "INSERT INTO dim_pet_category (pet_category_key, pet_category_name) VALUES (?, ?) "
                            + "ON CONFLICT (pet_category_key) DO NOTHING");
            customerStatement = connection.prepareStatement(
                    "INSERT INTO dim_customer "
                            + "(customer_key, first_name, last_name, age, email, postal_area_key, pet_type, pet_name, pet_breed) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (customer_key) DO NOTHING");
            sellerStatement = connection.prepareStatement(
                    "INSERT INTO dim_seller (seller_key, first_name, last_name, email, postal_area_key) "
                            + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (seller_key) DO NOTHING");
            supplierStatement = connection.prepareStatement(
                    "INSERT INTO dim_supplier "
                            + "(supplier_key, supplier_name, contact_name, email, phone, address, city_key) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (supplier_key) DO NOTHING");
            storeStatement = connection.prepareStatement(
                    "INSERT INTO dim_store (store_key, store_name, store_location, city_key, phone, email) "
                            + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (store_key) DO NOTHING");
            productStatement = connection.prepareStatement(
                    "INSERT INTO dim_product "
                            + "(product_key, product_name, product_category_key, pet_category_key, supplier_key, "
                            + "product_price, stock_quantity, product_weight, product_color, product_size, product_brand, "
                            + "product_material, product_description, product_rating, product_reviews, product_release_date, product_expiry_date) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                            + "ON CONFLICT (product_key) DO NOTHING");
            dateStatement = connection.prepareStatement(
                    "INSERT INTO dim_date "
                            + "(date_key, full_date, year_number, quarter_number, month_number, day_number) "
                            + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (date_key) DO NOTHING");
            factStatement = connection.prepareStatement(
                    "INSERT INTO fact_sales "
                            + "(source_raw_id, source_file, source_file_number, source_sale_id, source_customer_id, "
                            + "source_seller_id, source_product_id, customer_key, seller_key, product_key, store_key, "
                            + "date_key, sale_quantity, sale_total_price, unit_product_price) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                            + "ON CONFLICT (source_raw_id) DO NOTHING");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            JsonNode event = mapper.readTree(value);

            String customerCountry = text(event, "customer_country");
            String sellerCountry = text(event, "seller_country");
            String storeCountry = text(event, "store_country");
            String supplierCountry = text(event, "supplier_country");
            String customerPostalAreaKey = key(customerCountry, text(event, "customer_postal_code"));
            String sellerPostalAreaKey = key(sellerCountry, text(event, "seller_postal_code"));
            String storeCityKey = key(text(event, "store_city"), text(event, "store_state"), storeCountry);
            String supplierCityKey = key(text(event, "supplier_city"), "", supplierCountry);
            String productCategory = text(event, "product_category");
            String petCategory = text(event, "pet_category");
            String customerKey = key(
                    text(event, "customer_first_name"), text(event, "customer_last_name"), text(event, "customer_age"),
                    text(event, "customer_email"), customerCountry, text(event, "customer_postal_code"),
                    text(event, "customer_pet_type"), text(event, "customer_pet_name"), text(event, "customer_pet_breed"));
            String sellerKey = key(
                    text(event, "seller_first_name"), text(event, "seller_last_name"), text(event, "seller_email"),
                    sellerCountry, text(event, "seller_postal_code"));
            String supplierKey = key(
                    text(event, "supplier_name"), text(event, "supplier_contact"), text(event, "supplier_email"),
                    text(event, "supplier_phone"), text(event, "supplier_address"), text(event, "supplier_city"),
                    supplierCountry);
            String storeKey = key(
                    text(event, "store_name"), text(event, "store_location"), text(event, "store_city"),
                    text(event, "store_state"), storeCountry, text(event, "store_phone"), text(event, "store_email"));
            String productKey = key(
                    text(event, "product_name"), productCategory, text(event, "product_price"),
                    text(event, "product_quantity"), petCategory, text(event, "product_weight"),
                    text(event, "product_color"), text(event, "product_size"), text(event, "product_brand"),
                    text(event, "product_material"), text(event, "product_description"), text(event, "product_rating"),
                    text(event, "product_reviews"), text(event, "product_release_date"),
                    text(event, "product_expiry_date"), text(event, "supplier_name"), text(event, "supplier_email"));
            LocalDate saleDate = parseDate(text(event, "sale_date"));
            Integer dateKey = saleDate == null ? null : Integer.valueOf(saleDate.format(DateTimeFormatter.BASIC_ISO_DATE));

            try {
                insertCountry(customerCountry);
                insertCountry(sellerCountry);
                insertCountry(storeCountry);
                insertCountry(supplierCountry);
                insertPostalArea(customerPostalAreaKey, customerCountry, text(event, "customer_postal_code"));
                insertPostalArea(sellerPostalAreaKey, sellerCountry, text(event, "seller_postal_code"));
                insertCity(storeCityKey, text(event, "store_city"), text(event, "store_state"), storeCountry);
                insertCity(supplierCityKey, text(event, "supplier_city"), null, supplierCountry);
                insertCategory(productCategoryStatement, productCategory, productCategory);
                insertCategory(petCategoryStatement, petCategory, petCategory);
                insertCustomer(event, customerKey, customerPostalAreaKey);
                insertSeller(event, sellerKey, sellerPostalAreaKey);
                insertSupplier(event, supplierKey, supplierCityKey);
                insertStore(event, storeKey, storeCityKey);
                insertProduct(event, productKey, productCategory, petCategory, supplierKey);
                insertDate(saleDate, dateKey);
                insertFact(event, customerKey, sellerKey, productKey, storeKey, dateKey);
                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
        }

        private void insertCountry(String country) throws SQLException {
            if (country == null || country.isBlank()) {
                return;
            }
            countryStatement.setString(1, country);
            countryStatement.setString(2, country);
            countryStatement.executeUpdate();
        }

        private void insertPostalArea(String postalAreaKey, String countryKey, String postalCode) throws SQLException {
            postalAreaStatement.setString(1, postalAreaKey);
            postalAreaStatement.setString(2, blankToNull(countryKey));
            postalAreaStatement.setString(3, blankToNull(postalCode));
            postalAreaStatement.executeUpdate();
        }

        private void insertCity(String cityKey, String cityName, String stateName, String countryKey) throws SQLException {
            cityStatement.setString(1, cityKey);
            cityStatement.setString(2, blankToNull(cityName));
            cityStatement.setString(3, blankToNull(stateName));
            cityStatement.setString(4, blankToNull(countryKey));
            cityStatement.executeUpdate();
        }

        private void insertCategory(PreparedStatement statement, String key, String name) throws SQLException {
            if (key == null || key.isBlank()) {
                return;
            }
            statement.setString(1, key);
            statement.setString(2, name);
            statement.executeUpdate();
        }

        private void insertCustomer(JsonNode event, String customerKey, String postalAreaKey) throws SQLException {
            customerStatement.setString(1, customerKey);
            customerStatement.setString(2, blankToNull(text(event, "customer_first_name")));
            customerStatement.setString(3, blankToNull(text(event, "customer_last_name")));
            customerStatement.setObject(4, parseInteger(text(event, "customer_age")));
            customerStatement.setString(5, blankToNull(text(event, "customer_email")));
            customerStatement.setString(6, postalAreaKey);
            customerStatement.setString(7, blankToNull(text(event, "customer_pet_type")));
            customerStatement.setString(8, blankToNull(text(event, "customer_pet_name")));
            customerStatement.setString(9, blankToNull(text(event, "customer_pet_breed")));
            customerStatement.executeUpdate();
        }

        private void insertSeller(JsonNode event, String sellerKey, String postalAreaKey) throws SQLException {
            sellerStatement.setString(1, sellerKey);
            sellerStatement.setString(2, blankToNull(text(event, "seller_first_name")));
            sellerStatement.setString(3, blankToNull(text(event, "seller_last_name")));
            sellerStatement.setString(4, blankToNull(text(event, "seller_email")));
            sellerStatement.setString(5, postalAreaKey);
            sellerStatement.executeUpdate();
        }

        private void insertSupplier(JsonNode event, String supplierKey, String cityKey) throws SQLException {
            supplierStatement.setString(1, supplierKey);
            supplierStatement.setString(2, blankToNull(text(event, "supplier_name")));
            supplierStatement.setString(3, blankToNull(text(event, "supplier_contact")));
            supplierStatement.setString(4, blankToNull(text(event, "supplier_email")));
            supplierStatement.setString(5, blankToNull(text(event, "supplier_phone")));
            supplierStatement.setString(6, blankToNull(text(event, "supplier_address")));
            supplierStatement.setString(7, cityKey);
            supplierStatement.executeUpdate();
        }

        private void insertStore(JsonNode event, String storeKey, String cityKey) throws SQLException {
            storeStatement.setString(1, storeKey);
            storeStatement.setString(2, blankToNull(text(event, "store_name")));
            storeStatement.setString(3, blankToNull(text(event, "store_location")));
            storeStatement.setString(4, cityKey);
            storeStatement.setString(5, blankToNull(text(event, "store_phone")));
            storeStatement.setString(6, blankToNull(text(event, "store_email")));
            storeStatement.executeUpdate();
        }

        private void insertProduct(JsonNode event, String productKey, String productCategory, String petCategory, String supplierKey)
                throws SQLException {
            productStatement.setString(1, productKey);
            productStatement.setString(2, blankToNull(text(event, "product_name")));
            productStatement.setString(3, blankToNull(productCategory));
            productStatement.setString(4, blankToNull(petCategory));
            productStatement.setString(5, supplierKey);
            productStatement.setBigDecimal(6, parseDecimal(text(event, "product_price")));
            productStatement.setObject(7, parseInteger(text(event, "product_quantity")));
            productStatement.setBigDecimal(8, parseDecimal(text(event, "product_weight")));
            productStatement.setString(9, blankToNull(text(event, "product_color")));
            productStatement.setString(10, blankToNull(text(event, "product_size")));
            productStatement.setString(11, blankToNull(text(event, "product_brand")));
            productStatement.setString(12, blankToNull(text(event, "product_material")));
            productStatement.setString(13, blankToNull(text(event, "product_description")));
            productStatement.setBigDecimal(14, parseDecimal(text(event, "product_rating")));
            productStatement.setObject(15, parseInteger(text(event, "product_reviews")));
            productStatement.setDate(16, sqlDate(parseDate(text(event, "product_release_date"))));
            productStatement.setDate(17, sqlDate(parseDate(text(event, "product_expiry_date"))));
            productStatement.executeUpdate();
        }

        private void insertDate(LocalDate date, Integer dateKey) throws SQLException {
            if (date == null || dateKey == null) {
                return;
            }
            dateStatement.setInt(1, dateKey);
            dateStatement.setDate(2, Date.valueOf(date));
            dateStatement.setInt(3, date.getYear());
            dateStatement.setInt(4, ((date.getMonthValue() - 1) / 3) + 1);
            dateStatement.setInt(5, date.getMonthValue());
            dateStatement.setInt(6, date.getDayOfMonth());
            dateStatement.executeUpdate();
        }

        private void insertFact(
                JsonNode event, String customerKey, String sellerKey, String productKey, String storeKey, Integer dateKey)
                throws SQLException {
            factStatement.setLong(1, Long.parseLong(text(event, "source_raw_id")));
            factStatement.setString(2, blankToNull(text(event, "source_file")));
            factStatement.setObject(3, parseInteger(text(event, "source_file_number")));
            factStatement.setObject(4, parseLong(text(event, "id")));
            factStatement.setObject(5, parseLong(text(event, "sale_customer_id")));
            factStatement.setObject(6, parseLong(text(event, "sale_seller_id")));
            factStatement.setObject(7, parseLong(text(event, "sale_product_id")));
            factStatement.setString(8, customerKey);
            factStatement.setString(9, sellerKey);
            factStatement.setString(10, productKey);
            factStatement.setString(11, storeKey);
            factStatement.setObject(12, dateKey);
            factStatement.setObject(13, parseInteger(text(event, "sale_quantity")));
            factStatement.setBigDecimal(14, parseDecimal(text(event, "sale_total_price")));
            factStatement.setBigDecimal(15, parseDecimal(text(event, "product_price")));
            factStatement.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            closeStatement(factStatement);
            closeStatement(dateStatement);
            closeStatement(productStatement);
            closeStatement(storeStatement);
            closeStatement(supplierStatement);
            closeStatement(sellerStatement);
            closeStatement(customerStatement);
            closeStatement(petCategoryStatement);
            closeStatement(productCategoryStatement);
            closeStatement(cityStatement);
            closeStatement(postalAreaStatement);
            closeStatement(countryStatement);
            if (connection != null) {
                connection.close();
            }
        }

        private static void closeStatement(PreparedStatement statement) throws SQLException {
            if (statement != null) {
                statement.close();
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            return value == null || value.isNull() ? "" : value.asText();
        }

        private static String blankToNull(String value) {
            return value == null || value.isBlank() ? null : value;
        }

        private static Integer parseInteger(String value) {
            return value == null || value.isBlank() ? null : Integer.valueOf(value);
        }

        private static Long parseLong(String value) {
            return value == null || value.isBlank() ? null : Long.valueOf(value);
        }

        private static BigDecimal parseDecimal(String value) {
            return value == null || value.isBlank() ? null : new BigDecimal(value);
        }

        private static LocalDate parseDate(String value) {
            if (value == null || value.isBlank()) {
                return null;
            }
            try {
                return LocalDate.parse(value, SOURCE_DATE);
            } catch (DateTimeParseException exception) {
                return null;
            }
        }

        private static Date sqlDate(LocalDate date) {
            return date == null ? null : Date.valueOf(date);
        }

        private static String key(String... values) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                String joined = String.join("|", normalize(values));
                return HexFormat.of().formatHex(digest.digest(joined.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exception) {
                throw new IllegalStateException("Cannot build key", exception);
            }
        }

        private static String[] normalize(String[] values) {
            String[] result = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                result[i] = values[i] == null ? "" : values[i];
            }
            return result;
        }
    }
}
