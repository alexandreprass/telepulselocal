import tkinter as tk
from tkinter import messagebox
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants
import logging
import asyncio

class AccountStatusManager:
    def __init__(self, main_app_ref):
        self.main_app = main_app_ref
        self.loop = self.main_app.loop
        self.manager_window = None
        self.account_tree = None

        self.setup_ui()
        self.load_accounts_to_tree() # Carrega inicialmente
        logging.debug("AccountStatusManager __init__ concluída.")

    def setup_ui(self):
        self.manager_window = Toplevel(master=self.main_app.root, title="Gerenciador de Status de Contas")
        self.manager_window.geometry("800x500") # Aumentei um pouco a largura
        self.manager_window.transient(self.main_app.root)
        self.manager_window.grab_set()
        # O protocolo WM_DELETE_WINDOW é definido na MainWindow ao abrir esta janela
        # para garantir que a referência self.main_app.account_status_manager_window_ref seja limpa.

        main_frame = ttk.Frame(self.manager_window, padding="10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(expand=True, fill=tk.BOTH, pady=(0, 10))

        columns = ("phone", "app_status", "connection_status")
        self.account_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", bootstyle=ttk_constants.PRIMARY)
        
        self.account_tree.heading("phone", text="Telefone")
        self.account_tree.heading("app_status", text="Status App (Uso em Ferramentas)")
        self.account_tree.heading("connection_status", text="Status Conexão Telegram")

        self.account_tree.column("phone", width=180, anchor=tk.W) # Ajustado
        self.account_tree.column("app_status", width=220, anchor=tk.CENTER) # Ajustado
        self.account_tree.column("connection_status", width=200, anchor=tk.CENTER) # Ajustado

        self.account_tree.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        self.account_tree.bind("<<TreeviewSelect>>", self.on_tree_select)


        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.account_tree.yview, bootstyle="round-primary")
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.account_tree.configure(yscrollcommand=scrollbar.set)

        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=5)

        self.toggle_app_status_button = ttk.Button(buttons_frame, text="Alternar Status App", command=self.toggle_selected_account_app_status, bootstyle=ttk_constants.INFO, state=tk.DISABLED)
        self.toggle_app_status_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.connect_button = ttk.Button(buttons_frame, text="Conectar", command=self.connect_selected_account, bootstyle=ttk_constants.SUCCESS, state=tk.DISABLED)
        self.connect_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.disconnect_button = ttk.Button(buttons_frame, text="Desconectar", command=self.disconnect_selected_account, bootstyle=ttk_constants.WARNING, state=tk.DISABLED)
        self.disconnect_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        
        self.refresh_button = ttk.Button(buttons_frame, text="Atualizar Lista", command=self.refresh_accounts_display, bootstyle=ttk_constants.SECONDARY)
        self.refresh_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)


    def load_accounts_to_tree(self):
        if not self.account_tree or not self.manager_window.winfo_exists():
            logging.debug("AccountStatusManager: Tentativa de carregar árvore, mas UI não pronta ou destruída.")
            return
            
        selected_iid = self.account_tree.focus() 
        
        for item in self.account_tree.get_children():
            self.account_tree.delete(item)

        for acc_data in self.main_app.accounts: 
            phone = acc_data.get('phone', 'N/A')
            app_status = acc_data.get('app_status', 'INATIVO') 
            
            client = acc_data.get('client')
            connection_status = "Desconectado"
            if client and client.is_connected():
                connection_status = "Conectado" 

            self.account_tree.insert("", tk.END, values=(phone, app_status, connection_status), iid=phone)
        
        if selected_iid and self.account_tree.exists(selected_iid): 
            self.account_tree.focus(selected_iid)
            self.account_tree.selection_set(selected_iid)
        else: # Se não havia seleção ou a seleção sumiu, limpa o foco para evitar estado inconsistente dos botões
            current_selection = self.account_tree.selection()
            if current_selection: # Desseleciona se algo estiver selecionado por padrão
                self.account_tree.selection_remove(current_selection)

        self.update_button_states()


    def get_selected_phone_from_tree(self):
        selected_items = self.account_tree.selection()
        if not selected_items:
            return None
        return selected_items[0] 

    def on_tree_select(self, event=None):
        self.update_button_states()

    def toggle_selected_account_app_status(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para alternar o status.", parent=self.manager_window)
            return

        account_data = self.main_app.get_account_by_phone(selected_phone)
        if account_data:
            current_status = account_data.get('app_status', 'INATIVO')
            new_status = 'ATIVO' if current_status == 'INATIVO' else 'INATIVO'
            account_data['app_status'] = new_status 
            self.main_app.save_accounts() 
            self.main_app.log_message(f"Status da app para conta {selected_phone} alterado para {new_status} via gerenciador.", "info")
            # A função save_accounts na main_app já chama refresh_account_status_manager_if_open,
            # que por sua vez chama self.refresh_accounts_display() -> self.load_accounts_to_tree()
            # Então a atualização da árvore já está coberta.
        else:
            messagebox.showerror("Erro", f"Conta {selected_phone} não encontrada.", parent=self.manager_window)


    def connect_selected_account(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para conectar.", parent=self.manager_window)
            return
        
        # Desabilita botões durante a tentativa para evitar cliques múltiplos
        self.connect_button.config(state=tk.DISABLED)
        self.disconnect_button.config(state=tk.DISABLED)
        
        self.main_app.connect_account_by_phone_from_manager(selected_phone)
        # A atualização da UI do AccountStatusManager (e botões) é feita
        # pela MainWindow através de refresh_account_status_manager_if_open -> self.load_accounts_to_tree


    def disconnect_selected_account(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para desconectar.", parent=self.manager_window)
            return
        
        self.connect_button.config(state=tk.DISABLED)
        self.disconnect_button.config(state=tk.DISABLED)

        self.main_app.disconnect_account_by_phone_from_manager(selected_phone)


    def refresh_accounts_display(self):
        logging.debug("AccountStatusManager: Solicitado refresh_accounts_display")
        self.load_accounts_to_tree()
        # self.main_app.update_ui_elements_state() # Não precisa chamar daqui, pois a main_app já se atualiza


    def update_button_states(self):
        if not self.manager_window.winfo_exists(): return # Não faz nada se a janela foi destruída

        selected_phone = self.get_selected_phone_from_tree()
        has_selection = bool(selected_phone)

        self.toggle_app_status_button.config(state=tk.NORMAL if has_selection else tk.DISABLED)
        
        can_connect = False
        can_disconnect = False

        if has_selection:
            account_data = self.main_app.get_account_by_phone(selected_phone)
            if account_data:
                client = account_data.get('client')
                if client and client.is_connected():
                    can_disconnect = True
                else: 
                    can_connect = True 
        
        self.connect_button.config(state=tk.NORMAL if can_connect and has_selection else tk.DISABLED)
        self.disconnect_button.config(state=tk.NORMAL if can_disconnect and has_selection else tk.DISABLED)


    def on_close(self): # Chamado pelo protocolo WM_DELETE_WINDOW da MainWindow
        logging.debug("AccountStatusManager on_close (protocolo WM_DELETE_WINDOW) chamado.")
        # A referência na MainWindow já é limpa pela lambda que chama este on_close
        if self.manager_window and self.manager_window.winfo_exists():
            self.manager_window.destroy()

# Para teste individual, se necessário
if __name__ == '__main__':
    class MockMainApp:
        def __init__(self):
            self.root = tk.Tk()
            Style(theme="darkly")
            self.accounts = [
                {'phone': '+123', 'api_id': '1', 'api_hash': 'a', 'app_status': 'ATIVO', 'client': None},
                {'phone': '+456', 'api_id': '2', 'api_hash': 'b', 'app_status': 'INATIVO', 'client': type('MockClient', (), {'is_connected': lambda: True})()}
            ]
            self.loop = asyncio.new_event_loop()
            threading.Thread(target=self.loop.run_forever, daemon=True).start()
            self.account_status_manager_window_ref = None

        def get_account_by_phone(self, phone):
            return next((acc for acc in self.accounts if acc['phone'] == phone), None)
        
        def save_accounts(self): 
            print(f"MockMainApp: save_accounts() - Contas: {self.accounts}")
            if self.account_status_manager_window_ref: # Simula o refresh
                self.account_status_manager_window_ref.refresh_accounts_display()

        def log_message(self, msg, level): print(f"LOG [{level.upper()}]: {msg}")
        
        def connect_account_by_phone_from_manager(self, phone): 
            print(f"MockMainApp: connect_account_by_phone_from_manager({phone})")
            acc = self.get_account_by_phone(phone)
            if acc and not (acc.get('client') and acc['client'].is_connected()):
                # Simula conexão
                class MockClientTrue:
                    def is_connected(self): return True
                    async def disconnect(self): print(f"MockClient {phone} disconnect"); return True
                acc['client'] = MockClientTrue()
                print(f"Mock: Conta {phone} agora 'conectada'")
            self.save_accounts() # Para forçar refresh

        def disconnect_account_by_phone_from_manager(self, phone): 
            print(f"MockMainApp: disconnect_account_by_phone_from_manager({phone})")
            acc = self.get_account_by_phone(phone)
            if acc and acc.get('client') and acc['client'].is_connected():
                # Simula desconexão
                class MockClientFalse:
                    def is_connected(self): return False
                acc['client'] = MockClientFalse()
                print(f"Mock: Conta {phone} agora 'desconectada'")
            self.save_accounts() # Para forçar refresh

        def update_ui_elements_state(self): print("MockMainApp: update_ui_elements_state()")
        
        def initialize_client_for_account_data(self, acc_data, connect_now=False):
            print(f"MockMainApp: initialize_client_for_account_data for {acc_data['phone']}")
            class MockClient:
                def __init__(self, p): self.phone = p; self._connected = False
                def is_connected(self): return self._connected
                async def connect(self): self._connected = True; print(f"MockClient {self.phone} connected"); await asyncio.sleep(0.1)
                async def disconnect(self): self._connected = False; print(f"MockClient {self.phone} disconnected"); await asyncio.sleep(0.1)
                async def is_user_authorized(self): return self._connected 
                async def send_code_request(self,p): print(f"Mock send_code_request {p}"); return type('obj', (), {'phone_code_hash': 'mock_hash'})()
                async def sign_in(self, phone=None, code=None, password=None, phone_code_hash=None): print(f"Mock sign_in {phone} {code} {password}"); await asyncio.sleep(0.1)
            if 'client' not in acc_data or acc_data['client'] is None:
                acc_data['client'] = MockClient(acc_data['phone'])


    mock_app = MockMainApp()
    manager = AccountStatusManager(mock_app)
    mock_app.account_status_manager_window_ref = manager 
    mock_app.root.mainloop()
