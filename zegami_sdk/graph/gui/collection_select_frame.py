from tkinter import Frame, OptionMenu, Button, Label, StringVar


class CollectionSelectFrame():

    def __init__(self, app):

        self.app = app
        self.root = Frame(self.app.root)
        self.vars = {
            'w_name': StringVar(self.root, 'Select', name='w_name'),
            'c_name': StringVar(self.root, 'Select', name='c_name'),
        }
        self.available_wids = [w.name for w in self.app.client.workspaces]
        self.available_cids = []
        self.build()

    def build(self):

        a = self.app
        r = self.root

        # Workspace ID
        Label(r, text='Workspace:').place(x=a.PAD_S, y=a.PAD_S, width=100, height=a.ROW_H)
        self._update_workspaces_menu()

        # Collection ID
        Label(r, text='Collection:').place(x=a.PAD_S, y=a.ROW_H + a.PAD_S, width=100, height=a.ROW_H)
        self._update_collections_menu()

        # Connect button
        x = Button(r, text='Connect', command=self._handle_connect)
        x.place(x=300 + a.PAD_S, y=a.PAD_S, width=100, height=2*a.ROW_H - a.PAD_S)

    def _update_workspaces_menu(self):
        a = self.app
        v = self.vars['w_name']
        x = OptionMenu(self.root, v, *self.scoped_workspace_names, command=self._handle_option_wid)
        x.place(x=100 + a.PAD_S, y=a.PAD_S, width=200, height=a.ROW_H)

    def _update_collections_menu(self):
        a = self.app
        v = self.vars['c_name']
        x = OptionMenu(self.root, v, None, *self.scoped_collection_names, command=self._handle_option_cid)
        x.place(x=100 + a.PAD_S, y=a.ROW_H + a.PAD_S, width=200, height=a.ROW_H)

    def _handle_option_wid(self, val):
        self.vars['w_name'].set(val)
        self.app.workspace = self.app.client.get_workspace_by_name(val)
        self.available_cids = [c.name for c in self.app.workspace.collections]
        self._update_collections_menu()

    def _handle_option_cid(self, val):
        self.vars['c_name'].set(val)

    def _handle_connect(self):

        wid = self.vars['w_name'].get()
        try:
            c = next(filter(lambda c: str(c) == self.vars['c_name'].get(), [c for c in self.app.workspace.collections]))
        except Exception as e:
            print('Failed to connect: {}'.format(e))
            Label(self.root, text='Failed to connect').grid(row=4, column=1)
            return

        self.app.collection = c

        x = Label(self.root, text='Connected to "{}"'.format(c.name))
        x.grid(row=4, column=1)

        self.app.on_connected(c)

    @property
    def scoped_workspace_names():
        pass

    @scoped_workspace_names.getter
    def scoped_workspace_names(self):
        return [w.name for w in self.app.client.workspaces]

    @property
    def scoped_collection_names():
        pass

    @scoped_collection_names.getter
    def scoped_collection_names(self):
        if self.app.workspace is None:
            return []
        return [str(c) for c in self.app.workspace.collections]
