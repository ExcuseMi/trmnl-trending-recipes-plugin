import sqlite3
c = sqlite3.connect('/data/recipes.db')
print("mv_recipe_ranks count:", c.execute('SELECT COUNT(*) FROM mv_recipe_ranks').fetchone()[0])
print("Top 5 in mv_recipe_ranks:")
for r in c.execute('SELECT recipe_id, popularity_score, global_rank FROM mv_recipe_ranks ORDER BY global_rank LIMIT 5'):
    print(r)
print("Recipe 230540 in mv_recipe_ranks:")
print(c.execute("SELECT recipe_id, popularity_score, global_rank FROM mv_recipe_ranks WHERE recipe_id='230540'").fetchone())
