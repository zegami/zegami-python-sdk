from tkinter import Frame, OptionMenu, Button, Label, StringVar


class GraphFrame():

    def __init__(self, app):

        self.app = app
        self.root = Frame(self.app.root)
        self.build()

    def build(self):

        a = self.app
        r = self.root
