import streamlit as st
import tempfile
from pyspark.sql import SparkSession
from helpers.utils import clean_plan, get_color, visualize_plan

spark = SparkSession.builder \
    .appName("SparkQueryAnalyzerUI") \
    .getOrCreate()

st.title("Spark Query Plan Analyzer")
st.write("Upload a dataset and analyze Spark execution plans.")

uploaded_file = st.file_uploader("Upload CSV Dataset", type=["csv"])

if uploaded_file is not None:

    # Save the uploaded file to temp location
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    # Read the dataset with Spark
    df = spark.read.csv(tmp_path, header=True, inferSchema=True)

    # Register a temp view for SQL queries
    df.createOrReplaceTempView("dataset")

    st.subheader("Dataset Preview")
    st.dataframe(df.limit(20).toPandas())

    st.subheader("Enter Spark SQL Query")

    query = st.text_area(
        "Example:",
        "SELECT country, SUM(amount) as total_revenue FROM dataset GROUP BY country"
    )

    if st.button("Analyze Query"):
        try:
            result = spark.sql(query)

            st.subheader("Query Result")
            st.dataframe(result.limit(20).toPandas())

            qe = result._jdf.queryExecution()

            logical_plan = qe.logical().toString()
            optimized_plan = qe.optimizedPlan().toString()
            physical_plan = qe.executedPlan().toString()

            st.subheader("Logical Plan")
            st.code(clean_plan(logical_plan), language="text")

            st.subheader("Optimized Plan")
            st.code(clean_plan(optimized_plan), language="text")

            st.subheader("Physical Plan")
            st.code(clean_plan(physical_plan), language="text")

            st.subheader("Query Analysis")

            if "FileScan" in physical_plan:
                st.write("File Scan detected")

            shuffle_count = physical_plan.count("Exchange")
            if shuffle_count > 0:
                st.write(f"Shuffle detected ({shuffle_count} shuffle stage)")

            if "HashAggregate" in physical_plan:
                st.write("Aggregation detected")

            if "BroadcastHashJoin" in physical_plan or "SortMergeJoin" in physical_plan:
                st.write("Join detected")

            if "Project" in physical_plan:
                st.write("Projection detected")

            st.subheader("Execution DAG")
            dag = visualize_plan(physical_plan)
            st.graphviz_chart(dag)

        except Exception as e:
            st.error(f"Query failed: {e}")
