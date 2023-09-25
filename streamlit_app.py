import duckdb
import streamlit as st


def get_shakespeare() -> duckdb.DuckDBPyRelation:
    return duckdb.read_parquet("data/shakespeare.parquet")


def main():
    st.markdown("# What did Shakespeare say about ...?")
    search = st.text_input("word", value="butter").strip()

    sharespeare = get_shakespeare()
    duckdb.register("sharespeare", sharespeare)
    duckdb.execute("CREATE TABLE IF NOT EXISTS corpus AS SELECT * from sharespeare;")
    try:
        duckdb.execute("PRAGMA create_fts_index('corpus', 'line_id', 'text_entry');")
    except duckdb.CatalogException:
        pass
    if search:
        result = duckdb.execute(
            """
    SELECT fts_main_corpus.match_bm25(line_id, ?) AS score,
        line_id, play_name, speaker, text_entry
      FROM corpus
      WHERE score IS NOT NULL
      ORDER BY score DESC;""",
            (search,),
        )
        st.table(result.df())


if __name__ == "__main__":
    main()
