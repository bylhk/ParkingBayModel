import streamlit as st

class PageLayout:
    def __init__(
        self
    ) -> None:
        self.pages = {}

    def add(
        self,
        category,
        name,
        func,
    ) -> None:
        if not self.pages.get(category):
            self.pages[category] = []
        
        self.pages[category].append(dict(name=name, func=func))
    
    def run(
        self
    ) -> None:
        category_box = st.sidebar.selectbox('Category', list(self.pages.keys()))
        page_box = st.sidebar.selectbox('Page', self.pages[category_box], format_func=lambda p: p['name'])
        page_box['func']()
