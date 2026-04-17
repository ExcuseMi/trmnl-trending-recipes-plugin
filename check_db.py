import sqlite3
c = sqlite3.connect('/data/recipes.db')
for r in c.execute('SELECT id, name, popularity_score FROM recipes ORDER BY popularity_score DESC LIMIT 5'):
    print(r)
