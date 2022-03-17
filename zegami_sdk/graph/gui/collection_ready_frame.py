from tkinter import Frame, Button, Label


class CollectionReadyFrame():

    def __init__(self, app):

        self.app = app
        self.root = Frame(self.app.root)
        self.build()

    def build(self):

        a = self.app
        r = self.root

        Label(r, text='Workspace: {}'.format(a.workspace.name)).place(x=a.PAD_S, y=a.PAD_S, width=300, height=a.ROW_H)
        Label(r, text='Collection: {}'.format(a.collection.name)).place(x=a.PAD_S, y=a.ROW_H + a.PAD_S, width=300, height=a.ROW_H)

        # Disconnect button
        x = Button(r, text='Disconnect', command=a.on_disconnected)
        x.place(x=300 + a.PAD_S, y=a.PAD_S, width=100, height=2*a.ROW_H - a.PAD_S)
