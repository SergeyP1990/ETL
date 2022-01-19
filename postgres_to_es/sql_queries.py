import datetime

from psycopg2 import sql

def fw_full_sql_query(updated_at: datetime.datetime, sql_limit: int):
    return sql.SQL(
        """
        SELECT
            fw.id as fw_id,
            fw.rating as imdb_rating,
            fw.title,
            fw.description,
            fw.updated_at,
            ARRAY_AGG(DISTINCT g.name ) AS "genres",
            ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'director') AS "director",
            ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'actor') AS "actors_names",
            ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'writer') AS "writers_names",
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.updated_at > {updated_at}
        GROUP BY fw_id, fw.updated_at
        ORDER BY fw.updated_at
        LIMIT {sql_limit};
        """
    ).format(updated_at=sql.Literal(updated_at),sql_limit=sql.Literal(sql_limit))

def fw_persons_sql_query(filmwork_ids: set):
    return sql.SQL(
    """
    SELECT
        fw.id as fw_id,
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'director') AS "director",
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'actor') AS "actors_names",
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'writer') AS "writers_names",
        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE fw.id IN ({filmwork_ids})
    GROUP BY fw_id;
    """
    ).format(filmwork_ids=sql.SQL(", ").join(map(sql.Literal, filmwork_ids)))

def fw_genres_sql_query(filmwork_ids: set):
    sql.SQL(
    """
        SELECT
            fw.id as fw_id,
            ARRAY_AGG(DISTINCT g.name ) AS "genres"
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({filmwork_ids})
        GROUP BY fw_id;
        """
    ).format(filmwork_ids=sql.SQL(", ").join(map(sql.Literal, filmwork_ids)))

def nested_pre_sql(table: str, updated_at: datetime.datetime, limit: int):
    return sql.SQL(
        """
        SELECT id, updated_at
        FROM content.{table}
        WHERE updated_at > {updated_at}
        ORDER BY updated_at
        LIMIT {limit};
    """
    ).format(table=sql.Identifier(table),
            updated_at=sql.Literal(updated_at),
            limit=sql.Literal(limit))

def nested_fw_ids_sql(related_table: str, related_id: str, data_name_ids: set,
                      updated_at_rfw: datetime.datetime, limit: int):
    return sql.SQL(
    """
    SELECT fw.id, fw.updated_at
    FROM content.film_work fw
    LEFT JOIN content.{related_table} rfw ON rfw.film_work_id = fw.id
    WHERE rfw.{related_id} IN ({data_name_ids}) AND fw.updated_at > {updated_at_rfw}
    ORDER BY fw.updated_at
    LIMIT {limit};
    """
    ).format(related_table = sql.Identifier(related_table),
            related_id = sql.Identifier(related_id),
            data_name_ids = sql.SQL(", ").join(map(sql.Literal, data_name_ids)),
            updated_at_rfw = sql.Literal(updated_at_rfw),
            limit=sql.Literal(limit)
            )