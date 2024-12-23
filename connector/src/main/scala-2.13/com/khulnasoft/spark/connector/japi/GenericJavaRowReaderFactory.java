package com.khulnasoft.spark.connector.japi;

import com.khulnasoft.oss.driver.api.core.cql.Row;
import com.khulnasoft.spark.connector.CassandraRowMetadata;
import com.khulnasoft.spark.connector.ColumnRef;
import com.khulnasoft.spark.connector.cql.TableDef;
import com.khulnasoft.spark.connector.rdd.reader.RowReader;
import com.khulnasoft.spark.connector.rdd.reader.RowReaderFactory;
import scala.Option;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

public class GenericJavaRowReaderFactory {
    public final static RowReaderFactory<CassandraRow> instance = new RowReaderFactory<CassandraRow>() {

        @Override
        public RowReader<CassandraRow> rowReader(TableDef table, IndexedSeq<ColumnRef> selectedColumns) {
            return JavaRowReader.instance;
        }

        @Override
        public Class<CassandraRow> targetClass() {
            return CassandraRow.class;
        }
    };


    public static class JavaRowReader implements RowReader<CassandraRow> {
        public final static JavaRowReader instance = new JavaRowReader();

        private JavaRowReader() {
        }

        @Override
        public CassandraRow read(Row row, CassandraRowMetadata metaData) {
            assert row.getColumnDefinitions().size() == metaData.columnNames().size() :
                    "Number of columns in a row must match the number of columns in the table metadata";
            return CassandraRow$.MODULE$.fromJavaDriverRow(row, metaData);
        }

        @Override
        public Option<Seq<ColumnRef>> neededColumns() {
            return Option.empty();
        }
    }

}
