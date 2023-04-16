-- select the most popular song (1st place) in global chart
SELECT count(*) as top_count, CONCAT(artist, ' - ', title) as song FROM `sacred-alloy-375819.spotify.spotify_data`
    where region = "Global" and rank = 1 and chart = "top200" group by title, artist, url order by count(*) desc;

-- select the most popular artist (1st place) in global top200 chart
SELECT count(*) as top_count, artist FROM `sacred-alloy-375819.spotify.spotify_data`
    where region = "Global" and rank = 1 and chart = "top200" group by artist order by count(*) desc;

-- select the most popular song (1st place) in top200 chart by region
SELECT count(*) as top_count, CONCAT(artist, ' - ', title) as song, artist, title, region FROM `sacred-alloy-375819.spotify.spotify_data`
    where rank = 1 and chart = "top200" group by title, artist, url, region order by count(*) desc;

-- select the most popular artist (1st place) in top200 chart by region
SELECT count(*) as top_count, artist, region FROM `sacred-alloy-375819.spotify.spotify_data`
    where rank = 1 and chart = "top200" group by artist, region order by count(*) desc;
