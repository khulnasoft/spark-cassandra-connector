package com.khulnasoft.spark.connector.japi;

import com.khulnasoft.spark.connector.rdd.reader.RowReader;
import com.khulnasoft.spark.connector.writer.RowWriter;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import static com.khulnasoft.spark.connector.japi.CassandraJavaUtil.classTag;
import static com.khulnasoft.spark.connector.util.JavaApiHelper.toScalaFunction1;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import com.khulnasoft.spark.connector.ColumnSelector;
import com.khulnasoft.spark.connector.RDDFunctions;
import com.khulnasoft.spark.connector.cql.CassandraConnector;
import com.khulnasoft.spark.connector.cql.CassandraConnector$;
import com.khulnasoft.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.khulnasoft.spark.connector.rdd.*;
import com.khulnasoft.spark.connector.rdd.partitioner.CassandraPartitionedRDD;
import com.khulnasoft.spark.connector.rdd.reader.RowReaderFactory;
import com.khulnasoft.spark.connector.util.JavaApiHelper;
import com.khulnasoft.spark.connector.writer.RowWriterFactory;
import com.khulnasoft.spark.connector.writer.WriteConf;


/**
 * A Java API wrapper over {@link RDD} to provide Spark Cassandra Connector functionality.
 * <p/>
 * To obtain an instance of this wrapper, use one of the factory methods in {@link CassandraJavaUtil}
 * class.
 */
public class RDDJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final RDD<T> rdd;
    public final RDDFunctions<T> rddFunctions;

    public RDDJavaFunctions(RDD<T> rdd) {
        this.rdd = rdd;
        this.rddFunctions = new RDDFunctions<>(rdd);
    }

    public CassandraConnector defaultConnector() {
        return CassandraConnector$.MODULE$.apply(rdd.conf());
    }

    public SparkConf getConf() {
        return rdd.conf();
    }

    public void saveToCassandra(
            String keyspace,
            String table,
            RowWriterFactory<T> rowWriterFactory,
            ColumnSelector columnNames,
            WriteConf conf,
            CassandraConnector connector
    ) {
        rddFunctions.saveToCassandra(keyspace, table, columnNames, conf, connector, rowWriterFactory);
    }

    public void deleteFromCassandra(
            String keyspace,
            String table,
            RowWriterFactory<T> rowWriterFactory,
            ColumnSelector deleteColumns,
            ColumnSelector keyColumns,
            WriteConf conf,
            CassandraConnector connector
    ) {
        rddFunctions.deleteFromCassandra(keyspace, table, deleteColumns, keyColumns, conf, connector, rowWriterFactory);
    }
    /**
     * Applies a function to each item, and groups consecutive items having the same value together.
     * Contrary to {@code groupBy}, items from the same group must be already next to each other in the
     * original collection. Works locally on each partition, so items from different partitions will
     * never be placed in the same group.
     */
    public <U> JavaPairRDD<U, Iterable<T>> spanBy(final Function<T, U> f, ClassTag<U> keyClassTag) {
        ClassTag<Tuple2<U, Iterable<T>>> tupleClassTag = classTag(Tuple2.class);
        ClassTag<Iterable<T>> iterableClassTag = CassandraJavaUtil.classTag(Iterable.class);

        RDD<Tuple2<U, Iterable<T>>> newRDD = rddFunctions.spanBy(toScalaFunction1(f))
                .map(JavaApiHelper.<U, T, scala.collection.Iterable<T>>valuesAsJavaIterable(), tupleClassTag);

        return new JavaPairRDD<>(newRDD, keyClassTag, iterableClassTag);
    }

    /**
     * Uses the data from {@code RDD} to join with a Cassandra table without retrieving the entire table.
     * Any RDD which can be used to saveToCassandra can be used to joinWithCassandra as well as any RDD
     * which only specifies the partition Key of a Cassandra Table. This method executes single partition
     * requests against the Cassandra Table and accepts the functional modifiers that a normal
     * {@link CassandraTableScanRDD} takes.
     * <p/>
     * By default this method only uses the Partition Key for joining but any combination of columns
     * which are acceptable to C* can be used in the join. Specify columns using joinColumns as a
     * parameter or the {@code on()} method.
     */
    public <R> CassandraJavaPairRDD<T, R> joinWithCassandraTable(
            String keyspaceName,
            String tableName,
            ColumnSelector selectedColumns,
            ColumnSelector joinColumns,
            RowReaderFactory<R> rowReaderFactory,
            RowWriterFactory<T> rowWriterFactory
    ) {
        ClassTag<T> classTagT = rdd.toJavaRDD().classTag();
        ClassTag<R> classTagR = JavaApiHelper.getClassTag(rowReaderFactory.targetClass());

        CassandraConnector connector = defaultConnector();
        Option<ClusteringOrder> clusteringOrder = Option.empty();
        Option<CassandraLimit> limit = Option.empty();
        CqlWhereClause whereClause = CqlWhereClause.empty();
        ReadConf readConf = ReadConf.fromSparkConf(rdd.conf());

        CassandraJoinRDD<T, R> joinRDD = new CassandraJoinRDD<>(
                rdd,
                keyspaceName,
                tableName,
                connector,
                selectedColumns,
                joinColumns,
                whereClause,
                limit,
                clusteringOrder,
                readConf,
                Option.<RowReader<R>>empty(),
                Option.<RowWriter<T>>empty(),
                classTagT,
                classTagR,
                rowWriterFactory,
                rowReaderFactory);

        return new CassandraJavaPairRDD<>(joinRDD, classTagT, classTagR);
    }

    /**
     * Repartitions the data (via a shuffle) based upon the replication of the given {@code keyspaceName}
     * and {@code tableName}. Calling this method before using joinWithCassandraTable will ensure that
     * requests will be coordinator local. {@code partitionsPerHost} Controls the number of Spark
     * Partitions that will be created in this repartitioning event. The calling RDD must have rows that
     * can be converted into the partition key of the given Cassandra Table.
     */
    public JavaRDD<T> repartitionByCassandraReplica(
            String keyspaceName,
            String tableName,
            int partitionsPerHost,
            ColumnSelector partitionkeyMapper,
            RowWriterFactory<T> rowWriterFactory
    ) {
        CassandraConnector connector = defaultConnector();
        ClassTag<T> ctT = rdd.toJavaRDD().classTag();

        CassandraPartitionedRDD<T> newRDD = rddFunctions.repartitionByCassandraReplica(
                keyspaceName,
                tableName,
                partitionsPerHost,
                partitionkeyMapper,
                connector,
                ctT,
                rowWriterFactory);

        return new JavaRDD<>(newRDD, ctT);
    }

}



