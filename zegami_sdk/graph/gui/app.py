from tkinter import Tk, Label, Button, Entry, Frame, OptionMenu, StringVar,\
    Variable, LEFT, RIGHT

from zegami_sdk.client import ZegamiClient
from zegami_sdk.graph.graph import Graph
from collection_select_frame import CollectionSelectFrame
from collection_ready_frame import CollectionReadyFrame


class App():

    NAME = 'Zegami SDK Graph'
    PAD_S = 3
    PAD_M = 5
    PAD_L = 10
    ROW_H = 25
    COL_W = 50

    MODE_COLLECTION_SELECT = 0
    MODE_COLLECTION_READY = 1

    NOTIFY_NEUTRAL = 0
    NOTIFY_GOOD = 1
    NOTIFY_BAD = 2

    def __init__(self):

        self.client = ZegamiClient()
        self.workspace = None
        self.collection = None

        self.root = Tk()
        self.root.title(self.NAME)
        self.root.geometry('650x500')

        self.top_instance = None
        self.mode = None
        self.configure(self.MODE_COLLECTION_SELECT)

        self.notify_frame = Frame(self.root)
        h = self.ROW_H * 2 + self.PAD_S * 3
        self.notify_frame.place(x=400+6*self.PAD_S, y=self.PAD_S, width=200, height=h)
        self.notify_frame.configure(borderwidth=2, relief='groove')
        self.notify_last = None
        self.notify('Ready')

        self.root.mainloop()

    def on_connected(self, collection):
        self.configure(self.MODE_COLLECTION_READY)
        self.notify('Connected', self.NOTIFY_GOOD)

    def on_disconnected(self):
        self.workspace = None
        self.collection = None
        self.configure(self.MODE_COLLECTION_SELECT)
        self.notify('Disconnected', self.NOTIFY_NEUTRAL)

    def notify(self, message, notify_type=0):

        if notify_type == self.NOTIFY_NEUTRAL:
            color = 'black'
        elif notify_type == self.NOTIFY_GOOD:
            color = 'green'
        elif notify_type == self.NOTIFY_BAD:
            color = 'red'
        else:
            raise ValueError('Bad notify_type: {}'.format(notify_type))

        x = Label(self.notify_frame, text=message, highlightcolor=color)
        if self.notify_last:
            self.notify_last.destroy()
        self.notify_last = x
        self.notify_last.pack()

    def configure(self, mode):

        if mode == self.mode:
            return

        if self.top_instance is not None:
            self.top_instance.root.destroy()
            self.top_instance = None

        if mode == self.MODE_COLLECTION_SELECT:
            self.top_instance = CollectionSelectFrame(self)

        if mode == self.MODE_COLLECTION_READY:
            self.top_instance = CollectionReadyFrame(self)

        if self.top_instance is not None:
            h = self.ROW_H * 2 + self.PAD_S * 3
            self.top_instance.root.place(x=self.PAD_S, y=self.PAD_S, width=400+4*self.PAD_S, height=h)
            self.top_instance.root.configure(borderwidth=2, relief='groove')
        self.mode = mode


if __name__ == '__main__':
    App()
