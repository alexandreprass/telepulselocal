import tkinter as tk
from tkinter import messagebox, simpledialog
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants
import logging
import asyncio
import os

# Pyrogram não é diretamente usado aqui, mas as interações com self.main_app
# agora esperam que self.main_app.accounts[...]['client'] seja um cliente Pyrogram.

class AccountStatusManager:
    def __init__(self, main_app_ref):
        self.main_app = main_app_ref
        self.loop = self.main_app.loop # Usa o loop da MainWindow
        self.manager_window = None
        self.account_tree = None

        self.phone_entry_asm = None
        self.api_id_entry_asm = None
        self.api_hash_entry_asm = None

        self.setup_ui()
        self.load_accounts_to_tree()
        logging.debug("AccountStatusManager __init__ concluída.")

    def setup_ui(self):
        self.manager_window = Toplevel(master=self.main_app.root, title="Gerenciador de Status e Contas (Pyrogram)")
        self.manager_window.geometry("850x600")
        self.manager_window.transient(self.main_app.root)
        self.manager_window.grab_set()
        self.manager_window.protocol("WM_DELETE_WINDOW", self.on_window_close_protocol)

        outer_frame = ttk.Frame(self.manager_window, padding=0)
        outer_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        add_account_frame = ttk.LabelFrame(outer_frame, text="Adicionar ou Atualizar Conta", padding="10", bootstyle=ttk_constants.INFO)
        add_account_frame.pack(pady=(0,10), fill=tk.X)
        add_account_frame.columnconfigure(1, weight=1)

        ttk.Label(add_account_frame, text="Telefone (+CCCxxxxxxxx):").grid(row=0, column=0, padx=5, pady=3, sticky="w")
        self.phone_entry_asm = ttk.Entry(add_account_frame, width=30)
        self.phone_entry_asm.grid(row=0, column=1, padx=5, pady=3, sticky="ew")

        ttk.Label(add_account_frame, text="API ID:").grid(row=1, column=0, padx=5, pady=3, sticky="w")
        self.api_id_entry_asm = ttk.Entry(add_account_frame, width=30)
        self.api_id_entry_asm.grid(row=1, column=1, padx=5, pady=3, sticky="ew")

        ttk.Label(add_account_frame, text="API Hash:").grid(row=2, column=0, padx=5, pady=3, sticky="w")
        self.api_hash_entry_asm = ttk.Entry(add_account_frame, width=30)
        self.api_hash_entry_asm.grid(row=2, column=1, padx=5, pady=3, sticky="ew")

        self.add_update_button_asm = ttk.Button(add_account_frame, text="Adicionar / Atualizar Conta", command=self.add_or_update_account_from_manager, bootstyle=ttk_constants.SUCCESS)
        self.add_update_button_asm.grid(row=3, column=0, columnspan=2, padx=5, pady=10, sticky="ew")

        list_action_frame = ttk.LabelFrame(outer_frame, text="Contas Registradas", padding="10", bootstyle=ttk_constants.PRIMARY)
        list_action_frame.pack(expand=True, fill=tk.BOTH)

        tree_frame = ttk.Frame(list_action_frame)
        tree_frame.pack(expand=True, fill=tk.BOTH, pady=(0, 10))

        columns = ("phone", "app_status", "connection_status")
        self.account_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", bootstyle=ttk_constants.PRIMARY)

        self.account_tree.heading("phone", text="Telefone")
        self.account_tree.heading("app_status", text="Status App (Uso em Ferramentas)")
        self.account_tree.heading("connection_status", text="Status Conexão Telegram")

        self.account_tree.column("phone", width=180, anchor=tk.W)
        self.account_tree.column("app_status", width=220, anchor=tk.CENTER)
        self.account_tree.column("connection_status", width=200, anchor=tk.CENTER)

        self.account_tree.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        self.account_tree.bind("<<TreeviewSelect>>", self.on_tree_select)

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.account_tree.yview, bootstyle="round-primary")
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.account_tree.configure(yscrollcommand=scrollbar.set)

        buttons_frame = ttk.Frame(list_action_frame)
        buttons_frame.pack(fill=tk.X, pady=5)

        self.toggle_app_status_button = ttk.Button(buttons_frame, text="Alternar Status App", command=self.toggle_selected_account_app_status, bootstyle=ttk_constants.INFO, state=tk.DISABLED)
        self.toggle_app_status_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.connect_button = ttk.Button(buttons_frame, text="Conectar", command=self.connect_selected_account, bootstyle=ttk_constants.SUCCESS, state=tk.DISABLED)
        self.connect_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.disconnect_button = ttk.Button(buttons_frame, text="Desconectar", command=self.disconnect_selected_account, bootstyle=ttk_constants.WARNING, state=tk.DISABLED)
        self.disconnect_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.refresh_button = ttk.Button(buttons_frame, text="Atualizar Lista", command=self.refresh_accounts_display, bootstyle=ttk_constants.SECONDARY)
        self.refresh_button.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)

        self.remove_button_asm = ttk.Button(buttons_frame, text="Remover Selecionada", command=self.remove_selected_account_from_manager, bootstyle=ttk_constants.DANGER, state=tk.DISABLED)
        self.remove_button_asm.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)


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

            client = acc_data.get('client') # Pyrogram client
            connection_status = "Desconectado"
            if client and client.is_connected: # Pyrogram: .is_connected
                connection_status = "Conectado"

            self.account_tree.insert("", tk.END, values=(phone, app_status, connection_status), iid=phone)

        if selected_iid and self.account_tree.exists(selected_iid):
            self.account_tree.focus(selected_iid)
            self.account_tree.selection_set(selected_iid)
        else:
            current_selection = self.account_tree.selection()
            if current_selection:
                self.account_tree.selection_remove(current_selection)

        self.update_button_states()


    def get_selected_phone_from_tree(self):
        selected_items = self.account_tree.selection()
        if not selected_items:
            return None
        return selected_items[0] # O IID é o número de telefone

    def on_tree_select(self, event=None):
        self.update_button_states()
        selected_phone = self.get_selected_phone_from_tree()
        if selected_phone:
            account_data = self.main_app.get_account_by_phone(selected_phone)
            if account_data:
                self.phone_entry_asm.delete(0, tk.END)
                self.phone_entry_asm.insert(0, account_data.get('phone', ''))
                self.api_id_entry_asm.delete(0, tk.END)
                self.api_id_entry_asm.insert(0, account_data.get('api_id', ''))
                self.api_hash_entry_asm.delete(0, tk.END)
                self.api_hash_entry_asm.insert(0, account_data.get('api_hash', ''))


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
            self.main_app.save_accounts() # Salva a mudança de status
            self.main_app.log_message(f"Status da app para conta {selected_phone} alterado para {new_status} via gerenciador.", "info")
            self.load_accounts_to_tree() # Atualiza a árvore para refletir a mudança
        else:
            messagebox.showerror("Erro", f"Conta {selected_phone} não encontrada.", parent=self.manager_window)


    def connect_selected_account(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para conectar.", parent=self.manager_window)
            return

        self.connect_button.config(state=tk.DISABLED) # Desabilita durante a tentativa
        self.disconnect_button.config(state=tk.DISABLED) # Também

        # Chama o método da MainWindow que agora usa Pyrogram
        self.main_app.connect_account_by_phone_from_manager(selected_phone)
        # Os botões serão reabilitados/atualizados pela MainWindow após a tentativa de conexão


    def disconnect_selected_account(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para desconectar.", parent=self.manager_window)
            return

        self.connect_button.config(state=tk.DISABLED)
        self.disconnect_button.config(state=tk.DISABLED)

        # Chama o método da MainWindow que agora usa Pyrogram
        self.main_app.disconnect_account_by_phone_from_manager(selected_phone)
        # Os botões serão reabilitados/atualizados pela MainWindow


    def refresh_accounts_display(self):
        logging.debug("AccountStatusManager: Solicitado refresh_accounts_display")
        self.load_accounts_to_tree()


    def update_button_states(self):
        if not self.manager_window.winfo_exists(): return

        selected_phone = self.get_selected_phone_from_tree()
        has_selection = bool(selected_phone)

        self.toggle_app_status_button.config(state=tk.NORMAL if has_selection else tk.DISABLED)
        self.remove_button_asm.config(state=tk.NORMAL if has_selection else tk.DISABLED)

        can_connect = False
        can_disconnect = False

        if has_selection:
            account_data = self.main_app.get_account_by_phone(selected_phone)
            if account_data:
                client = account_data.get('client') # Pyrogram client
                if client and client.is_connected: # Pyrogram: .is_connected
                    can_disconnect = True
                else:
                    # Pode conectar se tiver os dados básicos (api_id, api_hash, phone)
                    if account_data.get('api_id') and account_data.get('api_hash') and account_data.get('phone'):
                        can_connect = True
        
        # Evita que o botão de conectar fique habilitado se o cliente já estiver conectado e vice-versa
        self.connect_button.config(state=tk.NORMAL if can_connect and has_selection and not can_disconnect else tk.DISABLED)
        self.disconnect_button.config(state=tk.NORMAL if can_disconnect and has_selection else tk.DISABLED)

    def add_or_update_account_from_manager(self):
        api_id = self.api_id_entry_asm.get()
        api_hash = self.api_hash_entry_asm.get()
        phone = self.phone_entry_asm.get()

        if not (api_id and api_hash and phone):
            messagebox.showerror("Erro", "Preencha API ID, API Hash e Telefone.", parent=self.manager_window)
            return
        if not phone.startswith("+"):
            messagebox.showerror("Erro de Formato", "O telefone deve iniciar com '+' seguido do código do país (ex: +5511987654321).", parent=self.manager_window)
            return
        try:
            int(api_id) # Valida se api_id é número
        except ValueError:
            messagebox.showerror("Erro", "API ID deve ser um número.", parent=self.manager_window)
            return

        existing_account_data = self.main_app.get_account_by_phone(phone)

        if existing_account_data:
            # Atualizar conta existente
            if messagebox.askyesno("Atualizar Conta", f"A conta {phone} já existe. Deseja atualizar o API ID e API Hash?\nA sessão atual será desconectada se estiver ativa e o arquivo de sessão será removido se API ID/Hash mudarem.", parent=self.manager_window):
                # Pyrogram: Desconectar se estiver conectado
                current_client = existing_account_data.get('client')
                if current_client and current_client.is_connected:
                    self.main_app.log_message(f"Desconectando {phone} (Pyrogram) para atualização via gerenciador...", "info")
                    # Usa o método da MainWindow para desconectar, que é async
                    self.main_app._initiate_disconnection_for_account(existing_account_data) # Isso é async, mas aqui é chamado de forma síncrona.
                                                                                             # A desconexão real ocorrerá no loop da main_app.
                                                                                             # Para garantir, podemos esperar um pouco ou redesenhar a árvore depois.

                # Pyrogram: Remover arquivo de sessão se API ID/Hash mudarem
                # O nome da sessão em Pyrogram é tipicamente o `name` (aqui, o número de telefone)
                session_file_pyrogram = f"{phone}.session" # Pyrogram adiciona .session
                session_file_telethon = f"session_{phone}.session" # Antigo arquivo Telethon

                api_id_changed = str(existing_account_data.get('api_id')) != api_id
                api_hash_changed = existing_account_data.get('api_hash') != api_hash

                if api_id_changed or api_hash_changed:
                    if os.path.exists(session_file_pyrogram):
                        try:
                            os.remove(session_file_pyrogram)
                            self.main_app.log_message(f"Arquivo de sessão Pyrogram {session_file_pyrogram} removido devido à mudança de API ID/Hash.", "info")
                        except Exception as e:
                            self.main_app.log_message(f"Erro ao remover arquivo de sessão Pyrogram {session_file_pyrogram}: {e}", "error")
                    if os.path.exists(session_file_telethon): # Tenta remover o antigo também
                        try:
                            os.remove(session_file_telethon)
                            self.main_app.log_message(f"Arquivo de sessão Telethon {session_file_telethon} removido.", "info")
                        except Exception as e:
                             self.main_app.log_message(f"Erro ao remover arquivo de sessão Telethon {session_file_telethon}: {e}", "error")


                existing_account_data['api_id'] = api_id
                existing_account_data['api_hash'] = api_hash
                # Pyrogram: A (re)inicialização do cliente é feita pela MainWindow.
                # Apenas atualizamos os dados aqui. A MainWindow usará esses dados ao conectar.
                # Se o cliente antigo existia, ele será substituído na próxima inicialização.
                existing_account_data.pop('client', None) # Remove a instância antiga do cliente para forçar reinicialização com novos dados

                self.main_app.save_accounts()
                self.main_app.log_message(f"Conta {phone} (Pyrogram) atualizada via gerenciador. Tente conectar se necessário.", 'info')
                self.load_accounts_to_tree() # Atualiza a UI
                self.main_app.update_account_menu_combobox()
            else:
                self.main_app.log_message(f"Atualização da conta {phone} (Pyrogram) cancelada.", 'info')
        else:
            # Adicionar nova conta
            new_account_data = {'phone': phone, 'api_id': api_id, 'api_hash': api_hash, 'app_status': 'ATIVO'}
            # Pyrogram: A instância do cliente será criada pela MainWindow quando necessário (ao conectar)
            # self.main_app.initialize_client_for_account_data(new_account_data) # Não conecta agora
            self.main_app.accounts.append(new_account_data)
            self.main_app.save_accounts()
            self.main_app.log_message(f"Conta {phone} (Pyrogram) adicionada com status ATIVO. Tente conectar.", 'info')
            self.load_accounts_to_tree()
            self.main_app.update_account_menu_combobox()

        self.phone_entry_asm.delete(0, tk.END)
        self.api_id_entry_asm.delete(0, tk.END)
        self.api_hash_entry_asm.delete(0, tk.END)


    def remove_selected_account_from_manager(self):
        selected_phone = self.get_selected_phone_from_tree()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma conta na lista para remover.", parent=self.manager_window)
            return

        if messagebox.askyesno("Remover Conta", f"Tem certeza que deseja remover a conta {selected_phone} e sua sessão salva? Isso a removerá permanentemente da aplicação.", parent=self.manager_window):
            # MainWindow.remove_account_by_phone_from_manager já foi adaptado para Pyrogram
            self.main_app.remove_account_by_phone_from_manager(selected_phone)

            # Limpa os campos de entrada se a conta removida estava neles
            if self.phone_entry_asm.get() == selected_phone:
                self.phone_entry_asm.delete(0, tk.END)
                self.api_id_entry_asm.delete(0, tk.END)
                self.api_hash_entry_asm.delete(0, tk.END)
            # A árvore será atualizada pela MainWindow via save_accounts -> refresh_account_status_manager_if_open


    def on_window_close_protocol(self):
        if self.main_app:
            self.main_app.on_account_status_manager_close()
        if self.manager_window and self.manager_window.winfo_exists():
            self.manager_window.destroy()
