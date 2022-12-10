import streamlit as st

from page.layout import PageLayout
from page.home import home_page
from page.analysis import top_page, heatmap_page, similarity_page

st.set_page_config(page_title="Melbourne On-street Parking", layout='wide')

app = PageLayout()
app.add('Home', 'Home', home_page)
app.add('Analysis', 'Parking Heatmap', heatmap_page)
app.add('Analysis', 'Top N Parking Bays', top_page)
app.add('Analysis', 'Bay Similarity', similarity_page)


if __name__ == '__main__':
    app.run()
