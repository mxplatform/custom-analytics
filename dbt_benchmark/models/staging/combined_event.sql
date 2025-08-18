
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


SELECT * from {{ source("anx_qa_r_mercury", "g4_subscribers_new") }}
limit 100
