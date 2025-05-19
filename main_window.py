import tkinter as tk
from tkinter import messagebox, scrolledtext, simpledialog
from ttkbootstrap import Style, ttk, Window
from ttkbootstrap.constants import *
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, UserPrivacyRestrictedError, UserChannelsTooMuchError, UserKickedError, UserBannedInChannelError, ChatAdminRequiredError, UsernameNotOccupiedError
import asyncio
import time
import threading
import datetime
import json
import os
import logging
import traceback

from member_manager import MemberManager
from member_adder import MemberAdder
from account_status_manager import AccountStatusManager

try:
    import aiohttp
except ImportError:
    print("Erro: Biblioteca 'aiohttp' não está instalada. Instale com: pip install aiohttp")
    exit(1)

try:
    import ttkbootstrap
except ImportError:
    print("Erro: Biblioteca 'ttkbootstrap' não está instalada. Instale com: pip install ttkbootstrap")
    exit(1)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MainWindow:
    def __init__(self, root):
        self.root = root
        self.root.title("TelePulse - Mass Message Sender")
        self.root.geometry("900x750")
        self.style = Style(theme='darkly')
        self.style.configure('TLabel', font=('Helvetica', 12))
        self.style.configure('TButton', font=('Helvetica', 11), padding=10)
        self.style.configure('TEntry', font=('Helvetica', 11), padding=8)
        self.style.configure('TLabelframe', font=('Helvetica', 12, 'bold'), padding=10)

        # Inicializar referências de janela primeiro
        self.account_status_manager_window_ref = None 
        self.member_manager_instance = None
        self.member_adder_instance = None

        self.accounts = []
        self.client = None 
        self.chats = []
        self.selected_chats_details_mass_main = []
        self.running_mass_sending_main = False

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._start_async_loop, daemon=True, name="AsyncLoopThread")
        self.loop_thread.start()

        self.config_file = "config.json"
        self.accounts_file = "accounts.json"
        
        self.load_accounts() # Agora self.account_status_manager_window_ref existe
        self.load_config_values_for_entry_fields()

        self.account_var = tk.StringVar()
        self.api_id_entry = None
        self.api_hash_entry = None
        self.phone_entry = None
        self.account_menu_combobox = None
        self.chat_listbox = None
        self.log_text = None
        self.status_label = None
        
        self.setup_ui()
        self.setup_menu()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        logging.debug("MainWindow __init__ parcialmente concluída. Definindo conta selecionada inicial...")

        if self.accounts:
            if not self.account_var.get() and self.accounts:
                 self.account_var.set(self.accounts[0]['phone'])
            self.on_account_selection_change()
            if any(os.path.exists(f'session_{acc["phone"]}.session') for acc in self.accounts):
                self.log_message("Sessões e contas encontradas.", 'debug')
        else:
            self.log_message("Nenhuma conta configurada encontrada.", "info")
            self.update_account_menu_combobox()

        logging.debug("MainWindow __init__ concluída.")

    def _start_async_loop(self):
        logging.debug(f"Iniciando loop asyncio na thread: {threading.current_thread().name}")
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        except Exception as e:
            logging.critical(f"Exceção no _start_async_loop: {e}", exc_info=True)
        finally:
            if hasattr(self.loop, 'is_closed') and not self.loop.is_closed():
                pending = asyncio.all_tasks(self.loop)
                if pending:
                    logging.debug(f"Cancelando {len(pending)} tarefas pendentes no loop asyncio.")
                    for task in pending:
                        task.cancel()
                    try:
                        self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception as e_gather:
                         logging.error(f"Erro ao aguardar tarefas canceladas: {e_gather}")
                self.loop.close()
            logging.debug(f"Loop asyncio finalizado na thread: {threading.current_thread().name}")

    def on_closing(self):
        logging.debug("on_closing chamado...")
        self.running_mass_sending_main = False

        for account in self.accounts:
            client_obj = account.get('client')
            if client_obj and client_obj.is_connected():
                logging.debug(f"Tentando desconectar cliente da conta {account['phone']} em on_closing...")
                future = asyncio.run_coroutine_threadsafe(client_obj.disconnect(), self.loop)
                try:
                    future.result(timeout=5)
                    logging.info(f"Cliente da conta {account['phone']} desconectado.")
                except Exception as e:
                    logging.error(f"Erro ao desconectar cliente da conta {account['phone']}: {e}")
        
        self.save_accounts()

        if self.loop.is_running():
            logging.debug("Parando o event loop do asyncio em on_closing...")
            self.loop.call_soon_threadsafe(self.loop.stop)
        
        if self.loop_thread.is_alive():
            self.loop_thread.join(timeout=5)
            if self.loop_thread.is_alive():
                logging.warning("Thread do loop asyncio não finalizou a tempo.")

        logging.debug("Destruindo janela principal em on_closing.")
        self.root.destroy()

    def load_config_values_for_entry_fields(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.api_id_str_loaded = config.get('api_id', '')
                    self.api_hash_str_loaded = config.get('api_hash', '')
                    logging.debug("Valores de configuração (para campos de entrada API ID/Hash) carregados.")
            except Exception as e:
                logging.error(f"Erro ao carregar config_file: {e}", exc_info=True)
                self.api_id_str_loaded = ''
                self.api_hash_str_loaded = ''
        else:
            self.api_id_str_loaded = ''
            self.api_hash_str_loaded = ''

    def save_config_values_from_selected_account(self):
        selected_account_data = self.get_selected_account_data_in_combobox()
        if selected_account_data:
            config = {'api_id': selected_account_data.get('api_id',''), 'api_hash': selected_account_data.get('api_hash','')}
            try:
                with open(self.config_file, 'w') as f:
                    json.dump(config, f)
                logging.debug("Configuração (API ID/Hash da conta selecionada) salva para campos de entrada.")
            except Exception as e:
                logging.error(f"Erro ao salvar config: {e}", exc_info=True)

    def load_accounts(self):
        if os.path.exists(self.accounts_file):
            try:
                with open(self.accounts_file, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        self.accounts = []
                    else:
                        loaded_data = json.loads(content)
                        self.accounts = []
                        for acc_data in loaded_data:
                            acc_data.pop('client', None)
                            if 'app_status' not in acc_data:
                                acc_data['app_status'] = 'INATIVO' 
                            self.accounts.append(acc_data)
                        logging.debug(f"Contas carregadas (sem clientes instanciados): {len(self.accounts)}")
            except Exception as e:
                logging.error(f"Erro ao carregar contas: {e}", exc_info=True)
                self.accounts = []
        else:
            self.accounts = []
        
        for account_data in self.accounts:
            if not account_data.get('client'):
                 self.initialize_client_for_account_data(account_data, connect_now=False)
        self.refresh_account_status_manager_if_open()


    def save_accounts(self):
        try:
            accounts_to_save = []
            for acc in self.accounts:
                acc_copy = acc.copy()
                acc_copy.pop('client', None)
                accounts_to_save.append(acc_copy)

            with open(self.accounts_file, 'w') as f:
                json.dump(accounts_to_save, f, indent=2)
            logging.debug(f"Contas salvas ({len(accounts_to_save)}).")
        except Exception as e:
            logging.error(f"Erro ao salvar contas: {e}", exc_info=True)
        self.refresh_account_status_manager_if_open()


    def add_or_update_account(self):
        api_id = self.api_id_entry.get()
        api_hash = self.api_hash_entry.get()
        phone = self.phone_entry.get()

        if not (api_id and api_hash and phone):
            messagebox.showerror("Erro", "Preencha API ID, API Hash e Telefone.")
            return
        if not phone.startswith("+"):
            messagebox.showerror("Erro de Formato", "O telefone deve iniciar com '+' seguido do código do país (ex: +5511987654321).")
            return
        try:
            int(api_id)
        except ValueError:
            messagebox.showerror("Erro", "API ID deve ser um número.")
            return

        existing_account_data = self.get_account_by_phone(phone)

        if existing_account_data:
            if messagebox.askyesno("Atualizar Conta", f"A conta {phone} já existe. Deseja atualizar o API ID e API Hash? A sessão atual será desconectada se estiver ativa e o arquivo de sessão será removido se API ID/Hash mudarem."):
                if existing_account_data.get('client') and existing_account_data['client'].is_connected():
                    self.log_message(f"Desconectando {phone} para atualização...", "info")
                    future = asyncio.run_coroutine_threadsafe(existing_account_data['client'].disconnect(), self.loop)
                    try: future.result(timeout=10)
                    except Exception as e: self.log_message(f"Erro ao desconectar {phone} para atualização: {e}", "error")
                    existing_account_data['client'] = None

                if str(existing_account_data.get('api_id')) != api_id or existing_account_data.get('api_hash') != api_hash:
                    session_file = f'session_{phone}.session'
                    if os.path.exists(session_file):
                        try:
                            os.remove(session_file)
                            self.log_message(f"Arquivo de sessão {session_file} removido devido à mudança de API ID/Hash.", "info")
                        except Exception as e:
                            self.log_message(f"Erro ao remover arquivo de sessão {session_file}: {e}", "error")
                
                existing_account_data['api_id'] = api_id
                existing_account_data['api_hash'] = api_hash
                self.initialize_client_for_account_data(existing_account_data, connect_now=False)
                self.save_accounts()
                self.update_account_menu_combobox()
                self.account_var.set(phone)
                self.on_account_selection_change()
                self.log_message(f"Conta {phone} atualizada. Tente conectar se necessário.", 'info')
            else:
                self.log_message(f"Atualização da conta {phone} cancelada.", 'info')
        else:
            new_account_data = {'phone': phone, 'api_id': api_id, 'api_hash': api_hash, 'app_status': 'ATIVO'}
            self.initialize_client_for_account_data(new_account_data, connect_now=False)
            self.accounts.append(new_account_data)
            self.save_accounts()
            self.update_account_menu_combobox()
            self.account_var.set(phone)
            self.on_account_selection_change() 
            self.log_message(f"Conta {phone} adicionada com status ATIVO. Tente conectar.", 'info')
        self.refresh_account_status_manager_if_open()


    def remove_selected_account_from_combobox(self):
        selected_phone = self.account_var.get()
        if not selected_phone:
            messagebox.showwarning("Nenhuma Conta", "Nenhuma conta selecionada no combobox para remover.")
            return

        if messagebox.askyesno("Remover Conta", f"Tem certeza que deseja remover a conta {selected_phone} e sua sessão salva? Isso a removerá permanentemente da aplicação."):
            account_index = self.get_account_index_by_phone(selected_phone)
            if account_index is not None:
                account_to_remove = self.accounts.pop(account_index)
                
                client_obj = account_to_remove.get('client')
                if client_obj and client_obj.is_connected():
                    asyncio.run_coroutine_threadsafe(client_obj.disconnect(), self.loop)

                session_file = f'session_{selected_phone}.session'
                if os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                        self.log_message(f"Arquivo de sessão {session_file} removido.", 'info')
                    except Exception as e:
                        self.log_message(f"Erro ao remover arquivo de sessão {session_file}: {e}", 'error')
                
                self.save_accounts()
                self.log_message(f"Conta {selected_phone} removida permanentemente.", 'info')

                self.api_id_entry.delete(0, tk.END)
                self.api_hash_entry.delete(0, tk.END)
                self.phone_entry.delete(0, tk.END)
                self.client = None
                self.chats = []
                if self.chat_listbox: self.chat_listbox.delete(0, tk.END)

                self.update_account_menu_combobox() 
                self.on_account_selection_change() 
            else:
                self.log_message(f"Conta {selected_phone} não encontrada para remoção (erro interno).", 'error')
            self.refresh_account_status_manager_if_open()


    def get_account_index_by_phone(self, phone_number):
        for i, acc in enumerate(self.accounts):
            if acc['phone'] == phone_number:
                return i
        return None
    
    def get_account_by_phone(self, phone_number):
        for acc in self.accounts:
            if acc['phone'] == phone_number:
                return acc
        return None

    def get_selected_account_data_in_combobox(self):
        if not self.account_var or not self.accounts:
            return None
        selected_phone = self.account_var.get()
        if not selected_phone:
            return None
        return self.get_account_by_phone(selected_phone)

    def get_operable_accounts(self): 
        operable_accounts_data = []
        for acc_data in self.accounts:
            if acc_data.get('app_status') == 'ATIVO':
                if not acc_data.get('client'):
                    self.initialize_client_for_account_data(acc_data, connect_now=False)
                operable_accounts_data.append(acc_data)
        
        if not operable_accounts_data:
            self.log_message("Nenhuma conta com status ATIVO encontrada para operação.", "warning")
        return operable_accounts_data


    def update_account_menu_combobox(self):
        if not hasattr(self, 'account_menu_combobox') or not self.account_menu_combobox:
            logging.debug("update_account_menu_combobox: account_menu_combobox ainda não existe.")
            return
        
        account_phones = [acc['phone'] for acc in self.accounts]
        self.account_menu_combobox['values'] = account_phones
        current_selection = self.account_var.get()

        if self.accounts:
            if current_selection in account_phones:
                pass
            else:
                self.account_var.set(account_phones[0])
        else:
            self.account_var.set("")
        self.update_ui_elements_state()


    def setup_menu(self):
        menubar = tk.Menu(self.root)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Gerenciar Status de Contas", command=self.open_account_status_manager)
        tools_menu.add_separator()
        tools_menu.add_command(label="Gerenciar Membros de Grupos", command=self.open_member_manager)
        tools_menu.add_command(label="Adicionar Membros a Grupos", command=self.open_member_adder)
        menubar.add_cascade(label="Ferramentas", menu=tools_menu)
        self.root.config(menu=menubar)
        logging.debug("Menu configurado.")

    def setup_ui(self):
        logging.debug("Iniciando setup_ui.")
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        title_frame = ttk.Frame(main_frame)
        title_frame.grid(row=0, column=0, columnspan=2, pady=(0,15), sticky="ew")
        ttk.Label(title_frame, text="TelePulse", font=('Helvetica', 24, 'bold'), bootstyle=PRIMARY).pack(pady=5)

        left_frame_container = ttk.Frame(main_frame)
        left_frame_container.grid(row=1, column=0, sticky="nsew", padx=(0,5))
        main_frame.columnconfigure(0, weight=1)

        config_frame = ttk.LabelFrame(left_frame_container, text="Detalhes da Conta (para Adicionar/Atualizar)", padding="10", bootstyle=INFO)
        config_frame.grid(row=0, column=0, pady=5, sticky="ew")
        config_frame.columnconfigure(1, weight=1)
        ttk.Label(config_frame, text="API ID:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.api_id_entry = ttk.Entry(config_frame, width=35)
        self.api_id_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        if hasattr(self, 'api_id_str_loaded'): self.api_id_entry.insert(0, self.api_id_str_loaded)
        ttk.Label(config_frame, text="API Hash:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.api_hash_entry = ttk.Entry(config_frame, width=35)
        self.api_hash_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        if hasattr(self, 'api_hash_str_loaded'): self.api_hash_entry.insert(0, self.api_hash_str_loaded)
        ttk.Label(config_frame, text="Telefone (+CCCxxxxxxxx):").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.phone_entry = ttk.Entry(config_frame, width=35)
        self.phone_entry.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(config_frame, text="Adicionar/Atualizar Detalhes da Conta", command=self.add_or_update_account, bootstyle=SUCCESS).grid(row=3, column=0, columnspan=2, padx=5, pady=10, sticky="ew")

        active_account_frame = ttk.LabelFrame(left_frame_container, text="Gerenciamento da Conta Selecionada", padding="10", bootstyle=INFO)
        active_account_frame.grid(row=1, column=0, pady=10, sticky="ew")
        active_account_frame.columnconfigure(1, weight=1)
        ttk.Label(active_account_frame, text="Conta Selecionada:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.account_menu_combobox = ttk.Combobox(active_account_frame, textvariable=self.account_var, state="readonly", width=33)
        self.account_menu_combobox.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.account_menu_combobox.bind("<<ComboboxSelected>>", self.on_account_selection_change)
        self.connect_button = ttk.Button(active_account_frame, text="Conectar Conta Selecionada", command=self.connect_selected_client_threaded, bootstyle=SUCCESS)
        self.connect_button.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
        self.disconnect_button = ttk.Button(active_account_frame, text="Desconectar Conta Selecionada", command=self.disconnect_selected_client_threaded, bootstyle=WARNING)
        self.disconnect_button.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        self.remove_account_button = ttk.Button(active_account_frame, text="Remover Conta Selecionada (Permanente)", command=self.remove_selected_account_from_combobox, bootstyle=DANGER)
        self.remove_account_button.grid(row=2, column=0, columnspan=2, padx=5, pady=5, sticky="ew")

        groups_frame = ttk.LabelFrame(left_frame_container, text="Grupos/Canais (Conta Selecionada)", padding="10", bootstyle=INFO)
        groups_frame.grid(row=2, column=0, pady=5, sticky="nsew")
        groups_frame.columnconfigure(0, weight=1)
        groups_frame.rowconfigure(0, weight=1)
        left_frame_container.rowconfigure(2, weight=1)
        self.chat_listbox = tk.Listbox(groups_frame, selectmode=tk.MULTIPLE, width=40, height=10, font=('Helvetica', 11))
        self.chat_listbox.grid(row=0, column=0, padx=(5,0), pady=5, sticky="nsew")
        scrollbar = ttk.Scrollbar(groups_frame, orient=VERTICAL, command=self.chat_listbox.yview, bootstyle="round")
        scrollbar.grid(row=0, column=1, sticky="ns", pady=5, padx=(0,5))
        self.chat_listbox.config(yscrollcommand=scrollbar.set)
        self.reload_chats_button = ttk.Button(groups_frame, text="Recarregar Grupos/Canais da Conta Selecionada", command=self.reload_chats_for_selected_account, bootstyle=INFO)
        self.reload_chats_button.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")

        right_frame_container = ttk.Frame(main_frame)
        right_frame_container.grid(row=1, column=1, sticky="nsew", padx=(5,0))
        main_frame.columnconfigure(1, weight=2)
        right_frame_container.rowconfigure(2, weight=1)

        message_frame = ttk.LabelFrame(right_frame_container, text="Mensagem em Massa (para Conta Selecionada)", padding="10", bootstyle=INFO)
        message_frame.grid(row=0, column=0, pady=5, sticky="ew")
        message_frame.columnconfigure(0, weight=1)
        self.message_text = scrolledtext.ScrolledText(message_frame, width=50, height=8, font=('Helvetica', 11), wrap=tk.WORD)
        self.message_text.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        interval_frame = ttk.Frame(message_frame)
        interval_frame.grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Label(interval_frame, text="Intervalo (minutos):").pack(side=tk.LEFT, padx=5, pady=5)
        self.interval_entry = ttk.Entry(interval_frame, width=10)
        self.interval_entry.insert(0, "10")
        self.interval_entry.pack(side=tk.LEFT, padx=5, pady=5)
        send_buttons_frame = ttk.Frame(message_frame)
        send_buttons_frame.grid(row=2, column=0, pady=5, sticky="ew")
        self.start_sending_button = ttk.Button(send_buttons_frame, text="Iniciar Envio em Massa (Conta Selecionada)", command=self.start_mass_sending_main, bootstyle=PRIMARY)
        self.start_sending_button.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        self.stop_sending_button = ttk.Button(send_buttons_frame, text="Parar Envio (Conta Selecionada)", command=self.stop_mass_sending_main, bootstyle=DANGER)
        self.stop_sending_button.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        
        self.status_label = ttk.Label(right_frame_container, text="Status: Pronta.", bootstyle=INFO, font=('Helvetica', 12))
        self.status_label.grid(row=1, column=0, padx=5, pady=10, sticky="ew")
        
        log_frame = ttk.LabelFrame(right_frame_container, text="Log de Ações da Aplicação", padding="10", bootstyle=INFO)
        log_frame.grid(row=2, column=0, pady=5, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = scrolledtext.ScrolledText(log_frame, width=50, height=10, state='disabled', font=('Helvetica', 10), wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        self.update_account_menu_combobox()
        self.update_ui_elements_state()
        logging.debug("setup_ui concluída.")


    def update_ui_elements_state(self):
        selected_account_data = self.get_selected_account_data_in_combobox()
        is_selected_connected = selected_account_data and selected_account_data.get('client') and selected_account_data['client'].is_connected()
        has_accounts = bool(self.accounts)
        has_selection_in_combobox = bool(selected_account_data)

        if self.connect_button:
            self.connect_button.config(state=tk.NORMAL if has_selection_in_combobox and not is_selected_connected else tk.DISABLED)
        if self.disconnect_button:
            self.disconnect_button.config(state=tk.NORMAL if is_selected_connected else tk.DISABLED)
        if self.remove_account_button:
            self.remove_account_button.config(state=tk.NORMAL if has_selection_in_combobox else tk.DISABLED)

        can_load_chats = is_selected_connected
        can_send_mass_message = is_selected_connected and self.chats and self.message_text and self.message_text.get("1.0", tk.END).strip()

        if self.reload_chats_button:
            self.reload_chats_button.config(state=tk.NORMAL if can_load_chats else tk.DISABLED)
        if self.start_sending_button:
             self.start_sending_button.config(state=tk.NORMAL if can_send_mass_message else tk.DISABLED)
        if self.stop_sending_button:
            self.stop_sending_button.config(state=tk.NORMAL if self.running_mass_sending_main else tk.DISABLED)
        if self.account_menu_combobox:
            self.account_menu_combobox.config(state=tk.NORMAL if has_accounts else tk.DISABLED)
        
        self.refresh_account_status_manager_if_open()


    def refresh_account_status_manager_if_open(self):
        if self.account_status_manager_window_ref and \
           self.account_status_manager_window_ref.manager_window and \
           self.account_status_manager_window_ref.manager_window.winfo_exists():
            self.account_status_manager_window_ref.refresh_accounts_display()


    def open_account_status_manager(self):
        if self.account_status_manager_window_ref and self.account_status_manager_window_ref.manager_window.winfo_exists():
            self.account_status_manager_window_ref.manager_window.lift()
        else:
            self.account_status_manager_window_ref = AccountStatusManager(self)


    def on_account_status_manager_close(self, manager_instance): # Chamado pelo protocolo WM_DELETE_WINDOW do AccountStatusManager
        # manager_instance.on_close() # O on_close do ASM já faz o destroy e nullify
        if self.account_status_manager_window_ref == manager_instance: 
            self.account_status_manager_window_ref = None
            logging.debug("Referência para AccountStatusManager limpa na MainWindow.")


    def open_member_manager(self):
        operable_accounts = self.get_operable_accounts()
        if not operable_accounts:
            self.log_message("Nenhuma conta ATIVA para abrir o Gerenciador de Membros. Configure no 'Gerenciador de Status de Contas'.", 'error')
            messagebox.showerror("Erro", "Nenhuma conta está marcada como ATIVA. Vá em Ferramentas -> Gerenciar Status de Contas.")
            return
        
        # Verifica se já existe uma instância e se a janela ainda existe
        if self.member_manager_instance and \
           hasattr(self.member_manager_instance, 'member_window') and \
           self.member_manager_instance.member_window.winfo_exists():
            self.member_manager_instance.member_window.lift()
        else:
            self.member_manager_instance = MemberManager(self)


    def open_member_adder(self):
        operable_accounts = self.get_operable_accounts()
        if not operable_accounts:
            self.log_message("Nenhuma conta ATIVA para abrir o Adicionador de Membros. Configure no 'Gerenciador de Status de Contas'.", 'error')
            messagebox.showerror("Erro", "Nenhuma conta está marcada como ATIVA. Vá em Ferramentas -> Gerenciar Status de Contas.")
            return
        
        if self.member_adder_instance and \
           hasattr(self.member_adder_instance, 'adder_window') and \
           self.member_adder_instance.adder_window.winfo_exists():
            self.member_adder_instance.adder_window.lift()
        else:
            # Garante que o MemberManager foi instanciado se o MemberAdder precisar dele
            if not (self.member_manager_instance and \
                    hasattr(self.member_manager_instance, 'member_window') and \
                    self.member_manager_instance.member_window.winfo_exists()):
                # Tenta abrir o MemberManager silenciosamente ou avisa o usuário
                # self.open_member_manager() # Isso pode ser confuso para o usuário
                # if not (self.member_manager_instance and self.member_manager_instance.member_window.winfo_exists()):
                #     messagebox.showinfo("Aviso", "O Gerenciador de Membros precisa ser aberto pelo menos uma vez para carregar a lista de membros para o Adicionador.", parent=self.root)
                #     # Não prossegue se o MemberManager não puder ser instanciado ou não tiver sido aberto.
                #     # Ou, permitir abrir o Adder, mas ele mostrará lista vazia.
                pass # Permite abrir o Adder, ele lidará com a lista de membros vazia se for o caso.

            self.member_adder_instance = MemberAdder(self)


    def log_message(self, message, level='info'):
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        console_log_msg = f"[{timestamp_str}] {message}"
        
        log_level_map = {'info': logging.INFO, 'error': logging.ERROR, 'debug': logging.DEBUG, 'warning': logging.WARNING, 'critical': logging.CRITICAL}
        logging.log(log_level_map.get(level.lower(), logging.INFO), message)

        ui_log_msg = console_log_msg
        # Evita re-formatar logs de outras janelas se eles já contêm identificadores
        is_tool_log = "[MemberManager]" in message or "[MemberAdder]" in message or "[AccountStatusManager]" in message
        
        if not is_tool_log:
            if "Mensagem enviada para" in message : 
                 if "SUCESSO" not in message and "ERRO" not in message: 
                    parts = message.split("para ")
                    if len(parts) > 1:
                        group_name = parts[1].split(":")[0].strip() 
                        ui_log_msg = f"[{timestamp_str}] {group_name} - SUCESSO ✅"
            elif "Erro ao enviar para" in message:
                if "SUCESSO" not in message and "ERRO" not in message:
                    parts = message.split("para ")
                    if len(parts) > 1:
                        group_name_parts = parts[1].split(":")
                        group_name = group_name_parts[0].strip()
                        error_detail = group_name_parts[1].strip() if len(group_name_parts) > 1 else "Erro desconhecido"
                        ui_log_msg = f"[{timestamp_str}] {group_name} - ERRO ❌ ({error_detail})"

        def _log_to_ui_safe():
            try:
                if hasattr(self, 'log_text') and self.log_text and self.log_text.winfo_exists():
                    self.log_text.configure(state='normal')
                    self.log_text.insert(tk.END, f"{ui_log_msg}\n")
                    self.log_text.see(tk.END)
                    self.log_text.configure(state='disabled')
            except Exception as e_ui_log: logging.error(f"Erro ao logar na UI (_log_to_ui_safe): {e_ui_log}", exc_info=True)

        if hasattr(self, 'root') and self.root.winfo_exists():
            try: self.root.after(0, _log_to_ui_safe)
            except Exception as e_after_log: logging.error(f"Erro em self.root.after para log_message: {e_after_log}", exc_info=True)


    def update_status(self, message):
        def _update_status_ui_safe():
            try:
                if hasattr(self, 'status_label') and self.status_label and self.status_label.winfo_exists():
                    self.status_label.config(text=f"Status: {message}")
            except Exception as e_ui_status: logging.error(f"Erro ao atualizar status na UI (_update_status_ui_safe): {e_ui_status}", exc_info=True)

        if hasattr(self, 'root') and self.root.winfo_exists():
            try: self.root.after(0, _update_status_ui_safe)
            except Exception as e_after_status: logging.error(f"Erro em self.root.after para update_status: {e_after_status}", exc_info=True)


    def connect_selected_client_threaded(self):
        account_data = self.get_selected_account_data_in_combobox()
        if not account_data:
            messagebox.showerror("Erro", "Nenhuma conta selecionada no combobox para conectar.")
            return
        self._initiate_connection_for_account(account_data.copy())

    def connect_account_by_phone_from_manager(self, phone_to_connect):
        account_data = self.get_account_by_phone(phone_to_connect)
        if not account_data:
            self.log_message(f"Tentativa de conectar conta inexistente {phone_to_connect} via gerenciador.", "error")
            self.refresh_account_status_manager_if_open() # Atualiza para mostrar que falhou
            return
        
        self.log_message(f"MainWindow: Iniciando conexão para {phone_to_connect} (solicitado via gerenciador).", "info")
        self._initiate_connection_for_account(account_data.copy())


    def _initiate_connection_for_account(self, account_data_copy):
        phone = account_data_copy['phone']
        active_threads = [t.name for t in threading.enumerate()]
        thread_name = f"ConnectLoginThread_{phone}"
        if thread_name in active_threads:
            self.log_message(f"Thread de conexão para {phone} já está em execução. Aguarde.", "info")
            return

        account_in_main_list = self.get_account_by_phone(phone) # Pega a referência real
        if not account_in_main_list: # Segurança
            self.log_message(f"Conta {phone} não encontrada para iniciar conexão.", "error")
            return

        if account_in_main_list.get('client') and account_in_main_list['client'].is_connected():
            self.log_message(f"Conta {phone} já está conectada.", "info")
            self.refresh_account_status_manager_if_open()
            if self.account_var.get() == phone:
                 self.root.after(0, self.load_chats_for_selected_account)
            self.root.after(0, self.update_ui_elements_state)
            return

        # Passa a REFERÊNCIA da conta na lista principal para a thread,
        # para que o objeto 'client' seja atualizado diretamente nela.
        threading.Thread(target=self._connect_and_login_task_for_specific_account, 
                         args=(account_in_main_list,), daemon=True, name=thread_name).start()


    def disconnect_selected_client_threaded(self):
        account_data = self.get_selected_account_data_in_combobox()
        if not account_data:
             messagebox.showerror("Erro", "Nenhuma conta selecionada no combobox para desconectar.")
             return
        self._initiate_disconnection_for_account(account_data) # Passa a referência

    def disconnect_account_by_phone_from_manager(self, phone_to_disconnect):
        account_data = self.get_account_by_phone(phone_to_disconnect)
        if not account_data:
            self.log_message(f"Tentativa de desconectar conta inexistente {phone_to_disconnect} via gerenciador.", "error")
            self.refresh_account_status_manager_if_open()
            return
        self._initiate_disconnection_for_account(account_data) # Passa a referência

    def _initiate_disconnection_for_account(self, account_data_ref): # Recebe a referência
        client_to_disconnect = account_data_ref.get('client')
        phone_to_disconnect = account_data_ref.get('phone')

        if not client_to_disconnect or not client_to_disconnect.is_connected():
            self.log_message(f"Conta {phone_to_disconnect} não está conectada.", "info")
            self.refresh_account_status_manager_if_open()
            self.root.after(0, self.update_ui_elements_state)
            return
        
        self.update_status(f"Desconectando {phone_to_disconnect}...")

        async def do_disconnect():
            try:
                await client_to_disconnect.disconnect()
                self.log_message(f"Conta {phone_to_disconnect} desconectada.", 'info')
                self.update_status(f"{phone_to_disconnect} desconectado.")
            except Exception as e:
                self.log_message(f"Erro ao desconectar {phone_to_disconnect}: {e}", 'error')
                self.update_status(f"Erro ao desconectar {phone_to_disconnect}.")
            finally:
                self.root.after(0, self.update_ui_elements_state)
                if self.account_var.get() == phone_to_disconnect:
                    self.root.after(0, lambda: self.chat_listbox.delete(0, tk.END) if self.chat_listbox else None)
                    self.chats = []
                self.refresh_account_status_manager_if_open()
        
        asyncio.run_coroutine_threadsafe(do_disconnect(), self.loop)


    def _connect_and_login_task_for_specific_account(self, account_data_ref): # Recebe a REFERÊNCIA
        phone = account_data_ref['phone']
        api_id_from_ref = account_data_ref['api_id'] # Usado para (re)inicializar o cliente
        api_hash_from_ref = account_data_ref['api_hash'] # Usado para (re)inicializar o cliente
        
        self.log_message(f"Thread de conexão iniciada para conta específica {phone}.", 'debug')
        self.update_status(f"Conectando {phone}...")

        # Garante que o cliente seja (re)inicializado se necessário, usando os dados da referência
        if not account_data_ref.get('client') or \
           (hasattr(account_data_ref['client'], 'api_id') and account_data_ref['client'].api_id != int(api_id_from_ref)) or \
           (hasattr(account_data_ref['client'], 'api_hash') and account_data_ref['client'].api_hash != api_hash_from_ref) :
            self.initialize_client_for_account_data(account_data_ref, connect_now=False)

        temp_client = account_data_ref.get('client') 
        if not temp_client:
            self.log_message(f"Falha ao obter objeto cliente para {phone} após inicialização.", "error")
            self.update_status(f"Falha crítica ao conectar {phone}.")
            self.root.after(0, self.update_ui_elements_state)
            self.refresh_account_status_manager_if_open()
            return

        async def actual_connect_logic():
            try:
                api_id_int = int(api_id_from_ref) 
            except ValueError:
                self.log_message(f"API ID '{api_id_from_ref}' para {phone} não é um número. Conexão abortada.", 'error')
                self.root.after(0, lambda: messagebox.showerror("Erro de Configuração", f"API ID para {phone} deve ser um número.", parent=self.root))
                self.update_status(f"Falha ({phone}): API ID inválido.")
                return False

            try:
                if temp_client.is_connected():
                    if await temp_client.is_user_authorized():
                        self.log_message(f"Cliente para {phone} já conectado e autorizado.", "info")
                        return True
                    else: 
                        await temp_client.disconnect()

                self.log_message(f"Tentando conectar {phone} com API ID: {api_id_int}", 'debug')
                await temp_client.connect()
                self.log_message(f"Conexão física com {phone} estabelecida.", 'debug')

                if not await temp_client.is_user_authorized():
                    self.log_message(f"Usuário {phone} não autorizado. Iniciando processo de login...", 'info')
                    self.update_status(f"Enviando código para {phone}...")
                    try:
                        sent_code_obj = await temp_client.send_code_request(phone)
                    except FloodWaitError as e_flood_code:
                        self.log_message(f"FloodWait ao enviar código para {phone}: {e_flood_code.seconds}s", "error")
                        self.root.after(0, lambda s=e_flood_code.seconds: messagebox.showerror("Limite Excedido", f"Muitas tentativas de enviar código para {phone}. Aguarde {s} segundos.", parent=self.root))
                        self.update_status(f"Falha ({phone}): Flood no código ({e_flood_code.seconds}s).")
                        if temp_client.is_connected(): await temp_client.disconnect()
                        return False
                    except Exception as e_send_code:
                        self.log_message(f"Erro ao enviar código para {phone}: {e_send_code}", "error")
                        self.root.after(0, lambda err=str(e_send_code): messagebox.showerror("Erro de Login", f"Falha ao enviar código para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha ({phone}): Erro no código.")
                        if temp_client.is_connected(): await temp_client.disconnect()
                        return False

                    user_code = await self._ask_for_input_async("Código de Login", f"Insira o código enviado para {phone}:")
                    if not user_code:
                        self.log_message(f"Login para {phone} cancelado (sem código).", "info")
                        self.update_status(f"Login cancelado para {phone}.")
                        if temp_client.is_connected(): await temp_client.disconnect()
                        return False
                    
                    try:
                        self.update_status(f"Verificando código para {phone}...")
                        await temp_client.sign_in(phone=phone, code=user_code, phone_code_hash=sent_code_obj.phone_code_hash)
                    except SessionPasswordNeededError:
                        self.log_message(f"Senha 2FA é necessária para {phone}.", 'info')
                        self.update_status(f"Senha 2FA necessária para {phone}...")
                        password = await self._ask_for_input_async("Senha 2FA", f"Insira sua senha 2FA para {phone}:", show='*')
                        if not password:
                            self.log_message(f"Login para {phone} cancelado (sem senha 2FA).", "info")
                            self.update_status(f"Login 2FA cancelado para {phone}.")
                            if temp_client.is_connected(): await temp_client.disconnect()
                            return False
                        self.update_status(f"Verificando senha 2FA para {phone}...")
                        await temp_client.sign_in(password=password)
                    except FloodWaitError as e_flood_signin:
                        self.log_message(f"FloodWait durante sign_in para {phone}: {e_flood_signin.seconds}s", "error")
                        self.root.after(0, lambda s=e_flood_signin.seconds: messagebox.showerror("Limite Excedido", f"Muitas tentativas de login para {phone}. Aguarde {s} segundos.", parent=self.root))
                        self.update_status(f"Falha ({phone}): Flood no login ({e_flood_signin.seconds}s).")
                        if temp_client.is_connected(): await temp_client.disconnect()
                        return False
                    except Exception as e_signin:
                        self.log_message(f"Erro durante sign_in para {phone}: {e_signin}", "error")
                        self.root.after(0, lambda err=str(e_signin): messagebox.showerror("Erro de Login", f"Falha no login para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha no login para {phone}.")
                        if temp_client.is_connected(): await temp_client.disconnect()
                        return False

                if await temp_client.is_user_authorized():
                    self.log_message(f"Conta {phone} autorizada com sucesso!", 'info')
                    return True
                else:
                    self.log_message(f"Falha na autorização final para {phone} após tentativas.", 'error')
                    if temp_client.is_connected(): await temp_client.disconnect()
                    return False

            except FloodWaitError as e_flood:
                self.log_message(f"FloodWait durante conexão/login de {phone}: {e_flood.seconds}s", "error")
                self.root.after(0, lambda s=e_flood.seconds: messagebox.showerror("Limite Excedido", f"Muitas tentativas com {phone}. Aguarde {s} segundos.", parent=self.root))
                self.update_status(f"Falha ({phone}): Flood ({e_flood.seconds}s).")
                if temp_client.is_connected(): await temp_client.disconnect()
                return False
            except ConnectionError as e_conn: 
                self.log_message(f"Erro de conexão de rede para {phone}: {e_conn}", 'error')
                self.root.after(0, lambda err_str=str(e_conn): messagebox.showerror("Erro de Rede", f"Não foi possível conectar {phone}: {err_str}", parent=self.root))
                self.update_status(f"Falha ({phone}): Erro de rede.")
                return False
            except Exception as e_main_logic: 
                self.log_message(f"Erro na lógica de conexão/login para {phone}: {e_main_logic}", 'error')
                self.root.after(0, lambda err_str=str(e_main_logic): messagebox.showerror("Erro Inesperado", f"Erro com {phone}: {err_str}", parent=self.root))
                self.update_status(f"Falha ({phone}): Erro inesperado.")
                if temp_client and temp_client.is_connected():
                    try: await temp_client.disconnect()
                    except: pass
                return False

        future = asyncio.run_coroutine_threadsafe(actual_connect_logic(), self.loop)
        try:
            connection_successful = future.result(timeout=300) 
            if connection_successful:
                self.update_status(f"{phone} conectado.")
                if self.account_var.get() == phone:
                    self.root.after(0, self.load_chats_for_selected_account)
                if self.account_var.get() == phone:
                    self.root.after(0, self.save_config_values_from_selected_account)
            else:
                self.update_status(f"Falha na conexão de {phone}.")
        except asyncio.TimeoutError:
            self.log_message(f"Timeout geral no processo de conexão de {phone} (300s).", 'error')
            self.update_status(f"Falha ({phone}): Timeout na conexão.")
            if future.cancel(): logging.debug(f"Future de conexão para {phone} cancelada devido a timeout.")
        except Exception as e_future:
            self.log_message(f"Erro ao obter resultado da future de conexão para {phone}: {e_future}", 'error')
            self.update_status(f"Falha grave ({phone}): {e_future}")
        finally:
            self.root.after(0, self.update_ui_elements_state) 
            self.refresh_account_status_manager_if_open()


    def initialize_client_for_account_data(self, account_data_ref, connect_now=False):
        phone = account_data_ref['phone']
        api_id = account_data_ref['api_id']
        api_hash = account_data_ref['api_hash']
        session_file = f'session_{phone}.session'

        try:
            api_id_int = int(api_id)
        except ValueError:
            self.log_message(f"API ID '{api_id}' para {phone} não é um número. Não foi possível inicializar cliente.", 'error')
            account_data_ref['client'] = None 
            return

        account_data_ref['client'] = TelegramClient(session_file, api_id_int, api_hash, loop=self.loop)
        logging.debug(f"Objeto TelegramClient (re)inicializado para {phone}.")

        if connect_now:
             self.log_message(f"initialize_client_for_account_data: connect_now=True não é mais recomendado. Conecte explicitamente.", "warning")


    def on_account_selection_change(self, event=None):
        selected_phone = self.account_var.get()
        if not selected_phone:
            self.client = None
            if self.api_id_entry: self.api_id_entry.delete(0, tk.END)
            if self.api_hash_entry: self.api_hash_entry.delete(0, tk.END)
            if self.phone_entry: self.phone_entry.delete(0, tk.END)
            self.chats = []
            if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
            self.update_status("Nenhuma conta selecionada no combobox.")
            self.update_ui_elements_state()
            return

        account_data = self.get_selected_account_data_in_combobox()
        if account_data:
            self.log_message(f"Conta selecionada no combobox: {selected_phone}", "info")
            
            if self.api_id_entry: self.api_id_entry.delete(0, tk.END); self.api_id_entry.insert(0, account_data.get('api_id', ''))
            if self.api_hash_entry: self.api_hash_entry.delete(0, tk.END); self.api_hash_entry.insert(0, account_data.get('api_hash', ''))
            if self.phone_entry: self.phone_entry.delete(0, tk.END); self.phone_entry.insert(0, account_data.get('phone', ''))

            self.client = account_data.get('client') 

            if self.client and self.client.is_connected():
                self.update_status(f"Conta {selected_phone} (selecionada) está Conectada.")
                self.load_chats_for_selected_account()
            else:
                self.update_status(f"Conta {selected_phone} (selecionada) está Desconectada.")
                self.chats = []
                if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
            
            self.save_config_values_from_selected_account()
        else:
            self.log_message(f"Erro: Conta '{selected_phone}' selecionada no combobox mas não encontrada nos dados.", 'error')
            self.client = None
        
        self.update_ui_elements_state()


    async def _ask_for_input_async(self, title, prompt, show=None):
        response_container = [None]
        input_ready_event = asyncio.Event()

        def ask_in_ui_thread():
            res = simpledialog.askstring(title, prompt, show=show, parent=self.root) 
            response_container[0] = res
            self.loop.call_soon_threadsafe(input_ready_event.set)

        if hasattr(self, 'root') and self.root.winfo_exists():
            self.root.after(0, ask_in_ui_thread)
        else:
            logging.error("_ask_for_input_async: Root window não existe mais.")
            self.loop.call_soon_threadsafe(input_ready_event.set) 
            return None

        await input_ready_event.wait()
        return response_container[0]


    def reload_chats_for_selected_account(self):
        selected_account_data = self.get_selected_account_data_in_combobox()
        if not selected_account_data or not selected_account_data.get('client') or not selected_account_data['client'].is_connected():
            self.log_message("Cliente da conta selecionada não conectado. Não é possível recarregar grupos.", 'error')
            messagebox.showerror("Erro", "Conecte a conta selecionada no combobox primeiro.")
            return
        self.load_chats_for_selected_account()

    def load_chats_for_selected_account(self):
        if not self.client or not self.client.is_connected():
            self.log_message("Tentativa de carregar chats sem cliente conectado (conta selecionada).", 'warning')
            self.update_ui_elements_state()
            return

        if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
        self.chats = [] 
        self.update_status("Carregando grupos/canais da conta selecionada...")
        if self.reload_chats_button: self.reload_chats_button.config(state=tk.DISABLED)

        future = asyncio.run_coroutine_threadsafe(self._fetch_chats_async_for_selected_client(), self.loop)

        def on_chats_loaded_callback(ft):
            try:
                ft.result()
                self.log_message("Busca de chats da conta selecionada concluída.", 'debug')
                if not self.chats: self.update_status("Nenhum grupo/canal encontrado para a conta selecionada.")
                else: self.update_status(f"{len(self.chats)} grupos/canais carregados (conta selecionada).")
            except ConnectionError as e_conn_cb:
                self.log_message(f"Erro de conexão ao carregar chats (callback): {e_conn_cb}", 'error')
                self.update_status("Erro de conexão ao carregar chats.")
            except Exception as e:
                self.log_message(f"Erro final ao carregar chats (callback): {e}", 'error')
                self.update_status("Erro ao carregar grupos/canais.")
            finally:
                if hasattr(self, 'reload_chats_button') and self.reload_chats_button:
                     self.root.after(0, lambda: self.reload_chats_button.config(state=tk.NORMAL if self.client and self.client.is_connected() else tk.DISABLED))
                self.root.after(0, self.update_ui_elements_state)
        future.add_done_callback(on_chats_loaded_callback)


    async def _fetch_chats_async_for_selected_client(self):
        self.log_message("Iniciando _fetch_chats_async para conta selecionada...", 'debug')
        try:
            if not self.client or not self.client.is_connected():
                self.log_message("Cliente (conta selecionada) desconectado antes de buscar chats.", 'error')
                raise ConnectionError("Cliente (conta selecionada) desconectado.")
            
            dialog_count = 0
            async for dialog in self.client.iter_dialogs(limit=None):
                dialog_count +=1
                if dialog_count % 50 == 0: self.update_status(f"Analisando diálogo {dialog_count} (conta selecionada)...")

                if dialog.is_group or dialog.is_channel:
                    title = dialog.title if dialog.title else f"ID {dialog.id}"
                    self.chats.append({'id': dialog.id, 'title': title, 'entity': dialog.entity})
                    self.root.after(0, lambda t=title: self.chat_listbox.insert(tk.END, t) if self.chat_listbox else None)
            
            self.log_message(f"Busca de diálogos da conta selecionada finalizada. {len(self.chats)} grupos/canais encontrados.", 'info')
            if not self.chats: self.log_message("Nenhum grupo/canal qualificado encontrado para conta selecionada.", 'warning')
        except Exception as e:
            self.log_message(f"Erro em _fetch_chats_async (conta selecionada): {e}", 'error')


    def start_mass_sending_main(self):
        self.log_message("Iniciando start_mass_sending_main...", 'debug')
        if not self.client or not self.client.is_connected(): 
            self.log_message("Cliente (conta selecionada) não conectado.", 'error')
            messagebox.showerror("Erro", "Conecte a conta selecionada no combobox primeiro.")
            return

        message_text_content = self.message_text.get("1.0", tk.END).strip()
        selected_indices = self.chat_listbox.curselection()

        if not message_text_content:
            self.log_message("Mensagem vazia para envio em massa (MainWindow).", 'error')
            messagebox.showerror("Erro", "A mensagem não pode estar vazia.")
            return
        if not selected_indices:
            self.log_message("Nenhum grupo selecionado para envio em massa (MainWindow).", 'error')
            messagebox.showerror("Erro", "Selecione pelo menos um grupo/canal.")
            return
        try:
            interval_min = float(self.interval_entry.get())
            if interval_min < 0 : raise ValueError("Intervalo deve ser não-negativo")
        except ValueError:
            self.log_message("Intervalo inválido para envio em massa (MainWindow).", 'error')
            messagebox.showerror("Erro", "Intervalo inválido. Insira um número não-negativo.")
            return

        interval_sec = interval_min * 60
        self.selected_chats_details_mass_main = [{'id': self.chats[i]['id'], 'title': self.chats[i]['title']} for i in selected_indices]
        
        self.running_mass_sending_main = True
        if self.start_sending_button: self.start_sending_button.config(state=tk.DISABLED)
        if self.stop_sending_button: self.stop_sending_button.config(state=tk.NORMAL)
        self.log_message(f"Iniciando envio em massa (MainWindow) para {len(self.selected_chats_details_mass_main)} grupos/canais a cada {interval_min:.2f} minutos.", 'info')
        self.update_status("Iniciando envios em massa (MainWindow)...")

        asyncio.run_coroutine_threadsafe(
            self._sender_coro_mass_main(message_text_content, self.selected_chats_details_mass_main, interval_sec),
            self.loop
        )
        messagebox.showinfo("Iniciado", f"Envio em massa (MainWindow) iniciado para {len(self.selected_chats_details_mass_main)} grupos/canais.")

    async def _sender_coro_mass_main(self, message_to_send, chats_to_send_to_details, send_interval_seconds):
        while self.running_mass_sending_main:
            cycle_start_time = time.time()
            for chat_detail in chats_to_send_to_details:
                if not self.running_mass_sending_main: break
                chat_id = chat_detail['id']
                chat_title = chat_detail['title']
                
                self.update_status(f"Enviando (MainWindow) para {chat_title}...")
                try:
                    if not self.client or not self.client.is_connected():
                        self.log_message("Cliente (conta selecionada) desconectou durante o envio (MainWindow). Parando.", "error")
                        self.running_mass_sending_main = False; break
                    
                    await self.client.send_message(chat_id, message_to_send)
                    self.log_message(f"Mensagem (MainWindow) enviada para {chat_title}", "info")
                except FloodWaitError as e_flood:
                    self.log_message(f"FloodWait (MainWindow) ao enviar para {chat_title}: {e_flood.seconds}s. Pausando.", "error")
                    self.update_status(f"FloodWait ({e_flood.seconds}s) para {chat_title}. Aguardando...")
                    await asyncio.sleep(e_flood.seconds + 5)
                except (UserPrivacyRestrictedError, UserKickedError, UserBannedInChannelError, ChatAdminRequiredError, UsernameNotOccupiedError) as e_perm:
                    self.log_message(f"Erro de permissão (MainWindow) ao enviar para {chat_title}: {type(e_perm).__name__}. Pulando.", "warning")
                except Exception as e_send:
                    self.log_message(f"Erro desconhecido (MainWindow) ao enviar para {chat_title}: {e_send}", "error")
                
                if self.running_mass_sending_main and len(chats_to_send_to_details) > 1: 
                    await asyncio.sleep(max(1, int(send_interval_seconds / len(chats_to_send_to_details) / 2))) 

            if not self.running_mass_sending_main: break

            elapsed_time_cycle = time.time() - cycle_start_time
            wait_time = send_interval_seconds - elapsed_time_cycle
            if wait_time > 0:
                self.update_status(f"Aguardando próximo ciclo (MainWindow) ({wait_time/60:.1f} min)...")
                wait_until = time.time() + wait_time
                while self.running_mass_sending_main and time.time() < wait_until: await asyncio.sleep(1)
            else:
                 self.update_status(f"Ciclo (MainWindow) concluído. Preparando próximo...")
                 await asyncio.sleep(2)

        self.root.after(0, self._finalize_sending_mass_main)


    def _finalize_sending_mass_main(self):
        self.update_status("Envios em massa (MainWindow) parados." if not self.running_mass_sending_main else "Envios em massa (MainWindow) concluídos.")
        self.running_mass_sending_main = False
        self.update_ui_elements_state() 


    def stop_mass_sending_main(self):
        if self.running_mass_sending_main:
            self.running_mass_sending_main = False
            self.log_message("Parando envio em massa de mensagens (MainWindow)...", 'info')
        else:
            self.log_message("Nenhum processo de envio em massa (MainWindow) ativo para parar.", "info")


def main():
    root = Window(themename="darkly")
    try:
        app = MainWindow(root)
        root.mainloop()
    except Exception as e:
        logging.critical(f"ERRO CRÍTICO AO INICIAR A APLICAÇÃO: {e}", exc_info=True)
        try:
            err_root = tk.Tk()
            err_root.withdraw()
            messagebox.showerror("Erro Crítico na Inicialização",
                                 f"Falha grave ao iniciar a aplicação:\n\n{type(e).__name__}: {e}\n\n"
                                 "Verifique o console ou o arquivo de log para detalhes.")
            err_root.destroy()
        except Exception as e_msgbox:
            print(f"Não foi possível mostrar o messagebox de erro crítico: {e_msgbox}")
            print(f"ERRO CRÍTICO AO INICIAR A APLICAÇÃO: {e}\n{traceback.format_exc()}")


if __name__ == "__main__":
    log_file_path = "telegram_mass_sender_errors.log"
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(filename)s:%(lineno)d - %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.info("--- Logging para arquivo configurado. Nova sessão da aplicação iniciada. ---")
    except Exception as e_log_setup:
        print(f"Erro crítico ao configurar logging para arquivo: {e_log_setup}")
    
    main()
