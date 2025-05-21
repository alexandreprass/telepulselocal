import tkinter as tk
from tkinter import messagebox, scrolledtext, simpledialog
from ttkbootstrap import Style, ttk, Window
from ttkbootstrap import constants as ttk_constants
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, FloodWait, UserPrivacyRestricted, UserChannelsTooMuch, UserKicked, UserBannedInChannel,
    ChatAdminRequired, UsernameNotOccupied, PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    ApiIdInvalid, ApiIdPublishedFlood, UserDeactivatedBan
)
from pyrogram.enums import ChatType
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
        self.root.title("TelePulse - Mass Message Sender (Pyrogram)")
        self.root.geometry("950x700")
        self.style = Style(theme='darkly')
        self.style.configure('TLabel', font=('Helvetica', 12))
        self.style.configure('TButton', font=('Helvetica', 11), padding=10)
        self.style.configure('TEntry', font=('Helvetica', 11), padding=8)
        self.style.configure('TLabelframe', font=('Helvetica', 12, 'bold'), padding=10)
        border_color = self.style.colors.light
        self.style.configure('OuterBorder.TFrame', background=border_color)

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

        self.accounts_file = "accounts.json"

        self.load_accounts()

        self.account_var = tk.StringVar()
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
            self.log_message("Sessões e contas encontradas (verificação inicial).", 'debug')
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

        disconnect_tasks = []
        for account_data in self.accounts:
            client_obj = account_data.get('client')
            if client_obj and client_obj.is_connected:
                logging.debug(f"Tentando desconectar cliente Pyrogram da conta {account_data['phone']} em on_closing...")
                disconnect_tasks.append(client_obj.stop())

        if disconnect_tasks:
            async def disconnect_all():
                await asyncio.gather(*disconnect_tasks, return_exceptions=True)
                logging.info("Todos os clientes Pyrogram solicitados para desconexão.")

            future = asyncio.run_coroutine_threadsafe(disconnect_all(), self.loop)
            try:
                future.result(timeout=10)
            except Exception as e:
                logging.error(f"Erro ao aguardar desconexão dos clientes Pyrogram: {e}")

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
                        logging.debug(f"Contas carregadas (sem clientes Pyrogram instanciados): {len(self.accounts)}")
            except Exception as e:
                logging.error(f"Erro ao carregar contas: {e}", exc_info=True)
                self.accounts = []
        else:
            self.accounts = []
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
        self.update_account_menu_combobox()

    def remove_account_by_phone_from_manager(self, phone_to_remove):
        account_index = self.get_account_index_by_phone(phone_to_remove)
        if account_index is not None:
            account_data = self.accounts.pop(account_index)
            client_obj = account_data.get('client')
            if client_obj and client_obj.is_connected:
                async def stop_client():
                    await client_obj.stop()
                asyncio.run_coroutine_threadsafe(stop_client(), self.loop)

            session_file_pyrogram = f"{phone_to_remove}.session"
            session_file_telethon = f"session_{phone_to_remove}.session" 

            if os.path.exists(session_file_pyrogram):
                try:
                    os.remove(session_file_pyrogram)
                    journal_file_pyrogram = f"{phone_to_remove}.session-journal"
                    if os.path.exists(journal_file_pyrogram):
                        os.remove(journal_file_pyrogram)
                    self.log_message(f"Arquivo de sessão Pyrogram {session_file_pyrogram} (e journal) removido.", 'info')
                except Exception as e:
                    self.log_message(f"Erro ao remover arquivo de sessão Pyrogram {session_file_pyrogram}: {e}", 'error')
            if os.path.exists(session_file_telethon):
                 try:
                    os.remove(session_file_telethon)
                    self.log_message(f"Arquivo de sessão Telethon {session_file_telethon} removido.", 'info')
                 except Exception as e:
                    self.log_message(f"Erro ao remover arquivo de sessão Telethon {session_file_telethon}: {e}", 'error')

            self.save_accounts()
            self.log_message(f"Conta {phone_to_remove} removida permanentemente (via gerenciador).", 'info')

            if self.account_var.get() == phone_to_remove:
                self.client = None
                self.chats = []
                if self.chat_listbox: self.chat_listbox.delete(0, tk.END)

            self.update_account_menu_combobox()
        else:
            self.log_message(f"Conta {phone_to_remove} não encontrada para remoção (via gerenciador).", 'error')
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
                    self.initialize_client_for_account_data(acc_data)
                operable_accounts_data.append(acc_data)
        if not operable_accounts_data:
            self.log_message("Nenhuma conta com status ATIVO encontrada para operação.", "warning")
        return operable_accounts_data

    def update_account_menu_combobox(self):
        if not hasattr(self, 'account_menu_combobox') or not self.account_menu_combobox:
            logging.debug("update_account_menu_combobox: account_menu_combobox ainda não existe.")
            return

        current_selection_before_update = self.account_var.get()
        account_phones = [acc['phone'] for acc in self.accounts]
        self.account_menu_combobox['values'] = account_phones

        new_selection = ""
        if self.accounts:
            if current_selection_before_update in account_phones:
                new_selection = current_selection_before_update
            elif account_phones:
                new_selection = account_phones[0]

        if self.account_var.get() != new_selection:
            self.account_var.set(new_selection)
            self.on_account_selection_change()
        else:
            self.update_ui_elements_state()

    def setup_menu(self):
        menubar = tk.Menu(self.root)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Gerenciar Contas e Status", command=self.open_account_status_manager)
        tools_menu.add_separator()
        tools_menu.add_command(label="Gerenciar Membros de Grupos", command=self.open_member_manager)
        tools_menu.add_command(label="Adicionar Membros a Grupos", command=self.open_member_adder)
        menubar.add_cascade(label="Ferramentas", menu=tools_menu)
        self.root.config(menu=menubar)
        logging.debug("Menu configurado.")

    def setup_ui(self):
        logging.debug("Iniciando setup_ui.")
        outer_border_frame = ttk.Frame(self.root, style='OuterBorder.TFrame')
        outer_border_frame.pack(expand=True, fill=tk.BOTH, padx=15, pady=15)
        main_content_frame = ttk.Frame(outer_border_frame, padding="10")
        main_content_frame.pack(expand=True, fill=tk.BOTH)
        main_content_frame.rowconfigure(0, weight=0)
        main_content_frame.rowconfigure(1, weight=1)
        main_content_frame.columnconfigure(0, weight=1, minsize=350)
        main_content_frame.columnconfigure(1, weight=3)
        title_frame = ttk.Frame(main_content_frame)
        title_frame.grid(row=0, column=0, columnspan=2, pady=(0,15), sticky="ew")
        ttk.Label(title_frame, text="TelePulse (Pyrogram)", font=('Helvetica', 24, 'bold'), bootstyle=ttk_constants.PRIMARY).pack(pady=5)
        left_frame_container = ttk.Frame(main_content_frame)
        left_frame_container.grid(row=1, column=0, sticky="nsew", padx=(0,10))
        left_frame_container.columnconfigure(0, weight=1)
        left_frame_container.rowconfigure(0, weight=0)
        left_frame_container.rowconfigure(1, weight=1)
        active_account_frame = ttk.LabelFrame(left_frame_container, text="Gerenciamento da Conta Selecionada", padding="10", bootstyle=ttk_constants.INFO)
        active_account_frame.grid(row=0, column=0, pady=10, sticky="new")
        active_account_frame.columnconfigure(1, weight=1)
        ttk.Label(active_account_frame, text="Conta Selecionada:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.account_menu_combobox = ttk.Combobox(active_account_frame, textvariable=self.account_var, state="readonly", width=33)
        self.account_menu_combobox.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.account_menu_combobox.bind("<<ComboboxSelected>>", self.on_account_selection_change)
        self.connect_button = ttk.Button(active_account_frame, text="Conectar Conta Selecionada", command=self.connect_selected_client_threaded, bootstyle=ttk_constants.SUCCESS)
        self.connect_button.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
        self.disconnect_button = ttk.Button(active_account_frame, text="Desconectar Conta Selecionada", command=self.disconnect_selected_client_threaded, bootstyle=ttk_constants.WARNING)
        self.disconnect_button.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        groups_frame = ttk.LabelFrame(left_frame_container, text="Grupos/Canais (Conta Selecionada)", padding="10", bootstyle=ttk_constants.INFO)
        groups_frame.grid(row=1, column=0, pady=5, sticky="nsew")
        groups_frame.columnconfigure(0, weight=1)
        groups_frame.rowconfigure(0, weight=1)
        self.chat_listbox = tk.Listbox(groups_frame, selectmode=tk.MULTIPLE, width=40, height=10, font=('Helvetica', 11))
        self.chat_listbox.grid(row=0, column=0, padx=(5,0), pady=5, sticky="nsew")
        scrollbar = ttk.Scrollbar(groups_frame, orient=ttk_constants.VERTICAL, command=self.chat_listbox.yview, bootstyle="round")
        scrollbar.grid(row=0, column=1, sticky="ns", pady=5, padx=(0,5))
        self.chat_listbox.config(yscrollcommand=scrollbar.set)
        self.reload_chats_button = ttk.Button(groups_frame, text="Recarregar Grupos/Canais da Conta Selecionada", command=self.reload_chats_for_selected_account, bootstyle=ttk_constants.INFO)
        self.reload_chats_button.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")
        right_frame_container = ttk.Frame(main_content_frame)
        right_frame_container.grid(row=1, column=1, sticky="nsew", padx=(5,0))
        right_frame_container.columnconfigure(0, weight=1)
        right_frame_container.rowconfigure(0, weight=0)
        right_frame_container.rowconfigure(1, weight=0)
        right_frame_container.rowconfigure(2, weight=1)
        message_frame = ttk.LabelFrame(right_frame_container, text="Mensagem em Massa (para Conta Selecionada)", padding="10", bootstyle=ttk_constants.INFO)
        message_frame.grid(row=0, column=0, pady=5, sticky="new")
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
        self.start_sending_button = ttk.Button(send_buttons_frame, text="Iniciar Envio em Massa (Conta Selecionada)", command=self.start_mass_sending_main, bootstyle=ttk_constants.PRIMARY)
        self.start_sending_button.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        self.stop_sending_button = ttk.Button(send_buttons_frame, text="Parar Envio (Conta Selecionada)", command=self.stop_mass_sending_main, bootstyle=ttk_constants.DANGER)
        self.stop_sending_button.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        self.status_label = ttk.Label(right_frame_container, text="Status: Pronta.", bootstyle=ttk_constants.INFO, font=('Helvetica', 12))
        self.status_label.grid(row=1, column=0, padx=5, pady=10, sticky="ew")
        log_frame = ttk.LabelFrame(right_frame_container, text="Log de Ações da Aplicação", padding="10", bootstyle=ttk_constants.INFO)
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
        is_selected_connected = selected_account_data and \
                                selected_account_data.get('client') and \
                                selected_account_data['client'].is_connected
        has_accounts = bool(self.accounts)
        has_selection_in_combobox = bool(selected_account_data)
        if self.connect_button:
            self.connect_button.config(state=tk.NORMAL if has_selection_in_combobox and not is_selected_connected else tk.DISABLED)
        if self.disconnect_button:
            self.disconnect_button.config(state=tk.NORMAL if is_selected_connected else tk.DISABLED)
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
           hasattr(self.account_status_manager_window_ref, 'manager_window') and \
           self.account_status_manager_window_ref.manager_window.winfo_exists():
            self.account_status_manager_window_ref.refresh_accounts_display()

    def open_account_status_manager(self):
        if self.account_status_manager_window_ref and \
           hasattr(self.account_status_manager_window_ref, 'manager_window') and \
           self.account_status_manager_window_ref.manager_window.winfo_exists():
            self.account_status_manager_window_ref.manager_window.lift()
        else:
            self.account_status_manager_window_ref = AccountStatusManager(self)

    def on_account_status_manager_close(self):
        self.account_status_manager_window_ref = None
        logging.debug("Referência para AccountStatusManager limpa na MainWindow.")

    def open_member_manager(self):
        selected_acc_data = self.get_selected_account_data_in_combobox()
        can_extract_with_main_account = selected_acc_data and \
                                        selected_acc_data.get('client') and \
                                        selected_acc_data['client'].is_connected
        if not can_extract_with_main_account:
            self.log_message("A conta selecionada na Janela Principal precisa estar conectada para extrair membros.", 'error')
            messagebox.showerror("Erro", "Conecte a conta selecionada na Janela Principal para poder extrair membros.")
            return
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
            messagebox.showerror("Erro", "Nenhuma conta está marcada como ATIVA para adicionar membros. Vá em Ferramentas -> Gerenciar Status de Contas.")
            return
        if self.member_adder_instance and \
           hasattr(self.member_adder_instance, 'adder_window') and \
           self.member_adder_instance.adder_window.winfo_exists():
            self.member_adder_instance.adder_window.lift()
        else:
            self.member_adder_instance = MemberAdder(self)

    def log_message(self, message, level='info'):
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        console_log_msg = f"[{timestamp_str}] {message}"
        log_level_map = {'info': logging.INFO, 'error': logging.ERROR, 'debug': logging.DEBUG, 'warning': logging.WARNING, 'critical': logging.CRITICAL}
        logging.log(log_level_map.get(level.lower(), logging.INFO), message)
        ui_log_msg = console_log_msg
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
        self._initiate_connection_for_account(account_data)

    def connect_account_by_phone_from_manager(self, phone_to_connect):
        account_data = self.get_account_by_phone(phone_to_connect)
        if not account_data:
            self.log_message(f"Tentativa de conectar conta inexistente {phone_to_connect} via gerenciador.", "error")
            self.refresh_account_status_manager_if_open()
            return
        self.log_message(f"MainWindow: Iniciando conexão para {phone_to_connect} (solicitado via gerenciador).", "info")
        self._initiate_connection_for_account(account_data)

    def _initiate_connection_for_account(self, account_data_ref):
        phone = account_data_ref['phone']
        active_threads = [t.name for t in threading.enumerate()]
        thread_name = f"ConnectLoginThread_{phone}"
        if thread_name in active_threads:
            self.log_message(f"Thread de conexão para {phone} já está em execução. Aguarde.", "info")
            return
        if account_data_ref.get('client') and account_data_ref['client'].is_connected:
            self.log_message(f"Conta {phone} (Pyrogram) já está conectada.", "info")
            self.refresh_account_status_manager_if_open()
            if self.account_var.get() == phone:
                 self.root.after(0, self.load_chats_for_selected_account)
            self.root.after(0, self.update_ui_elements_state)
            return
        threading.Thread(target=self._connect_and_login_task_for_specific_account,
                         args=(account_data_ref,), daemon=True, name=thread_name).start()

    def disconnect_selected_client_threaded(self):
        account_data = self.get_selected_account_data_in_combobox()
        if not account_data:
             messagebox.showerror("Erro", "Nenhuma conta selecionada no combobox para desconectar.")
             return
        self._initiate_disconnection_for_account(account_data)

    def disconnect_account_by_phone_from_manager(self, phone_to_disconnect):
        account_data = self.get_account_by_phone(phone_to_disconnect)
        if not account_data:
            self.log_message(f"Tentativa de desconectar conta inexistente {phone_to_disconnect} via gerenciador.", "error")
            self.refresh_account_status_manager_if_open()
            return
        self._initiate_disconnection_for_account(account_data)

    def _initiate_disconnection_for_account(self, account_data_ref):
        client_to_disconnect = account_data_ref.get('client')
        phone_to_disconnect = account_data_ref.get('phone')
        if not client_to_disconnect or not client_to_disconnect.is_connected:
            self.log_message(f"Conta {phone_to_disconnect} (Pyrogram) não está conectada.", "info")
            self.refresh_account_status_manager_if_open()
            self.root.after(0, self.update_ui_elements_state)
            return
        self.update_status(f"Desconectando {phone_to_disconnect}...")
        async def do_disconnect():
            try:
                await client_to_disconnect.stop()
                self.log_message(f"Conta {phone_to_disconnect} (Pyrogram) desconectada.", 'info')
                self.update_status(f"{phone_to_disconnect} desconectado.")
            except Exception as e:
                self.log_message(f"Erro ao desconectar {phone_to_disconnect} (Pyrogram): {e}", 'error')
                self.update_status(f"Erro ao desconectar {phone_to_disconnect}.")
            finally:
                self.root.after(0, self.update_ui_elements_state)
                if self.account_var.get() == phone_to_disconnect:
                    self.root.after(0, lambda: self.chat_listbox.delete(0, tk.END) if self.chat_listbox else None)
                    self.chats = []
                self.refresh_account_status_manager_if_open()
        asyncio.run_coroutine_threadsafe(do_disconnect(), self.loop)

    def _connect_and_login_task_for_specific_account(self, account_data_ref):
        phone = account_data_ref['phone']
        api_id_from_ref = account_data_ref['api_id']
        api_hash_from_ref = account_data_ref['api_hash']
        self.log_message(f"Thread de conexão Pyrogram iniciada para conta específica {phone}.", 'debug')
        self.update_status(f"Conectando {phone} (Pyrogram)...")
        current_client = account_data_ref.get('client')
        if not current_client or \
           (current_client.api_id != int(api_id_from_ref)) or \
           (current_client.api_hash != api_hash_from_ref):
            self.initialize_client_for_account_data(account_data_ref)
            current_client = account_data_ref.get('client')
        if not current_client:
            self.log_message(f"Falha ao obter objeto cliente Pyrogram para {phone} após inicialização.", "error")
            self.update_status(f"Falha crítica ao conectar {phone}.")
            self.root.after(0, self.update_ui_elements_state)
            self.refresh_account_status_manager_if_open()
            return
        temp_client = current_client

        async def actual_connect_logic():
            try:
                api_id_int = int(api_id_from_ref)
            except ValueError:
                self.log_message(f"API ID '{api_id_from_ref}' para {phone} não é um número. Conexão abortada.", 'error')
                self.root.after(0, lambda: messagebox.showerror("Erro de Configuração", f"API ID para {phone} deve ser um número.", parent=self.root))
                self.update_status(f"Falha ({phone}): API ID inválido.")
                return False
            try:
                if not temp_client.is_connected:
                    self.log_message(f"Tentando conectar {phone} (Pyrogram) com API ID: {api_id_int}", 'debug')
                    await temp_client.connect()
                    self.log_message(f"Conexão física com {phone} (Pyrogram) estabelecida (ou tentativa).", 'debug')
                else:
                    if temp_client.is_initialized:
                        self.log_message(f"Cliente Pyrogram para {phone} já conectado e autorizado.", "info")
                        return True

                if not temp_client.is_initialized:
                    self.log_message(f"Usuário {phone} (Pyrogram) não autorizado. Iniciando processo de login...", 'info')
                    self.update_status(f"Enviando código para {phone}...")
                    try:
                        sent_code_obj = await temp_client.send_code(phone)
                    except FloodWait as e_flood_code:
                        self.log_message(f"FloodWait (Pyrogram) ao enviar código para {phone}: {e_flood_code.value}s", "error")
                        self.root.after(0, lambda s=e_flood_code.value: messagebox.showerror("Limite Excedido", f"Muitas tentativas de enviar código para {phone}. Aguarde {s} segundos.", parent=self.root))
                        self.update_status(f"Falha ({phone}): Flood no código ({e_flood_code.value}s).")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False
                    except PhoneNumberInvalid as e_phone_inv:
                        self.log_message(f"Número de telefone inválido para {phone}: {e_phone_inv}", "error")
                        self.root.after(0, lambda err=str(e_phone_inv): messagebox.showerror("Erro de Login", f"Número de telefone inválido para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha ({phone}): Número inválido.")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False
                    except Exception as e_send_code:
                        self.log_message(f"Erro ao enviar código para {phone} (Pyrogram): {e_send_code}", "error")
                        self.root.after(0, lambda err=str(e_send_code): messagebox.showerror("Erro de Login", f"Falha ao enviar código para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha ({phone}): Erro no código.")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False

                    user_code = await self._ask_for_input_async("Código de Login", f"Insira o código enviado para {phone}:")
                    if not user_code:
                        self.log_message(f"Login para {phone} (Pyrogram) cancelado (sem código).", "info")
                        self.update_status(f"Login cancelado para {phone}.")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False
                    try:
                        self.update_status(f"Verificando código para {phone}...")
                        signed_in_user = await temp_client.sign_in(
                            phone_number=phone,
                            phone_code_hash=sent_code_obj.phone_code_hash,
                            phone_code=user_code
                        )
                        if signed_in_user:
                            self.log_message(f"Pyrogram sign_in retornou usuário para {phone}. Tentando get_me().", "debug")
                            try:
                                me_user = await temp_client.get_me()
                                if me_user:
                                    self.log_message(f"get_me() bem-sucedido após sign_in para {phone}. Usuário: {me_user.username or me_user.id}. Estado is_initialized AGORA: {temp_client.is_initialized}", "debug")
                                else:
                                    self.log_message(f"get_me() após sign_in para {phone} não retornou usuário. Estado is_initialized: {temp_client.is_initialized}", "warning")
                            except Exception as e_get_me:
                                self.log_message(f"Erro ao chamar get_me() após sign_in para {phone}: {e_get_me}. Estado is_initialized: {temp_client.is_initialized}", "error")
                        else:
                            self.log_message(f"Pyrogram sign_in para {phone} não retornou usuário. Estado is_initialized: {temp_client.is_initialized}", "warning")

                    except SessionPasswordNeeded:
                        self.log_message(f"Senha 2FA é necessária para {phone} (Pyrogram).", 'info')
                        self.update_status(f"Senha 2FA necessária para {phone}...")
                        password = await self._ask_for_input_async("Senha 2FA", f"Insira sua senha 2FA para {phone}:", show='*')
                        if not password:
                            self.log_message(f"Login para {phone} (Pyrogram) cancelado (sem senha 2FA).", "info")
                            self.update_status(f"Login 2FA cancelado para {phone}.")
                            if temp_client.is_connected: await temp_client.disconnect()
                            return False
                        try:
                            self.update_status(f"Verificando senha 2FA para {phone}...")
                            checked_password_user = await temp_client.check_password(password=password)
                            if checked_password_user:
                                self.log_message(f"Pyrogram check_password retornou usuário para {phone}. Tentando get_me().", "debug")
                                try:
                                    me_user_2fa = await temp_client.get_me()
                                    if me_user_2fa:
                                        self.log_message(f"get_me() bem-sucedido após check_password para {phone}. Usuário: {me_user_2fa.username or me_user_2fa.id}. Estado is_initialized AGORA: {temp_client.is_initialized}", "debug")
                                    else:
                                        self.log_message(f"get_me() após check_password para {phone} não retornou usuário. Estado is_initialized: {temp_client.is_initialized}", "warning")
                                except Exception as e_get_me_2fa:
                                    self.log_message(f"Erro ao chamar get_me() após check_password para {phone}: {e_get_me_2fa}. Estado is_initialized: {temp_client.is_initialized}", "error")
                            else:
                                self.log_message(f"Pyrogram check_password para {phone} não retornou usuário. Estado is_initialized: {temp_client.is_initialized}", "warning")
                        except Exception as e_pwd:
                            self.log_message(f"Erro durante check_password para {phone} (Pyrogram): {e_pwd}", "error")
                            self.root.after(0, lambda err=str(e_pwd): messagebox.showerror("Erro de Login 2FA", f"Falha na senha 2FA para {phone}: {err}", parent=self.root))
                            self.update_status(f"Falha na senha 2FA para {phone}.")
                            if temp_client.is_connected: await temp_client.disconnect()
                            return False
                    except (PhoneCodeInvalid, PhoneCodeExpired) as e_code_err:
                        self.log_message(f"Erro de código ({type(e_code_err).__name__}) durante sign_in para {phone} (Pyrogram): {e_code_err}", "error")
                        self.root.after(0, lambda err=str(e_code_err): messagebox.showerror("Erro de Login", f"Código inválido ou expirado para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha ({phone}): Código inválido/expirado.")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False
                    except FloodWait as e_flood_signin:
                        self.log_message(f"FloodWait (Pyrogram) durante sign_in para {phone}: {e_flood_signin.value}s", "error")
                        self.root.after(0, lambda s=e_flood_signin.value: messagebox.showerror("Limite Excedido", f"Muitas tentativas de login para {phone}. Aguarde {s} segundos.", parent=self.root))
                        self.update_status(f"Falha ({phone}): Flood no login ({e_flood_signin.value}s).")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False
                    except Exception as e_signin:
                        self.log_message(f"Erro durante sign_in para {phone} (Pyrogram): {e_signin}", "error")
                        self.root.after(0, lambda err=str(e_signin): messagebox.showerror("Erro de Login", f"Falha no login para {phone}: {err}", parent=self.root))
                        self.update_status(f"Falha no login para {phone}.")
                        if temp_client.is_connected: await temp_client.disconnect()
                        return False

                # NOVA VERIFICAÇÃO DE TESTE E DECISÃO:
                authorization_successful_via_get_me = False
                try:
                    me_final_check = await temp_client.get_me()
                    if me_final_check:
                        self.log_message(f"VERIFICAÇÃO FINAL COM GET_ME: Sucesso! Usuário: {me_final_check.id}. Estado is_initialized: {temp_client.is_initialized}", "info")
                        authorization_successful_via_get_me = True
                        if not temp_client.is_initialized: 
                             self.log_message(f"AVISO: get_me() funcionou, mas is_initialized ainda é {temp_client.is_initialized}. Continuando baseado no sucesso do get_me().", "warning")
                    else: 
                        self.log_message(f"VERIFICAÇÃO FINAL COM GET_ME: Falhou (get_me não retornou usuário). Estado is_initialized: {temp_client.is_initialized}", "error")

                except Exception as e_final_get_me:
                    self.log_message(f"VERIFICAÇÃO FINAL COM GET_ME: Falhou com exceção! Erro: {e_final_get_me}. Estado is_initialized: {temp_client.is_initialized}", "error")
                
                if authorization_successful_via_get_me:
                    self.log_message(f"Conta {phone} (Pyrogram) considerada autorizada com sucesso baseado em get_me().", 'info')
                    return True
                else: 
                    self.log_message(f"Falha na autorização final para {phone} (Pyrogram) após tentativas (get_me falhou). Estado is_initialized: {temp_client.is_initialized}", 'error')
                    if temp_client.is_connected: await temp_client.disconnect()
                    return False
            except FloodWait as e_flood:
                self.log_message(f"FloodWait (Pyrogram) durante conexão/login de {phone}: {e_flood.value}s", "error")
                self.root.after(0, lambda s=e_flood.value: messagebox.showerror("Limite Excedido", f"Muitas tentativas com {phone}. Aguarde {s} segundos.", parent=self.root))
                self.update_status(f"Falha ({phone}): Flood ({e_flood.value}s).")
                if temp_client.is_connected: await temp_client.disconnect()
                return False
            except (ApiIdInvalid, ApiIdPublishedFlood) as e_api:
                self.log_message(f"Erro de API ID/Hash para {phone} (Pyrogram): {e_api}", "error")
                self.root.after(0, lambda err=str(e_api): messagebox.showerror("Erro de Configuração", f"API ID/Hash inválido ou bloqueado para {phone}: {err}", parent=self.root))
                self.update_status(f"Falha ({phone}): API ID/Hash inválido.")
                if temp_client.is_connected: await temp_client.disconnect()
                return False
            except ConnectionError as e_conn:
                self.log_message(f"Erro de conexão de rede para {phone} (Pyrogram): {e_conn}", 'error')
                self.root.after(0, lambda err_str=str(e_conn): messagebox.showerror("Erro de Rede", f"Não foi possível conectar {phone}: {err_str}", parent=self.root))
                self.update_status(f"Falha ({phone}): Erro de rede.")
                return False
            except Exception as e_main_logic:
                self.log_message(f"Erro na lógica de conexão/login para {phone} (Pyrogram): {e_main_logic}", 'error')
                traceback.print_exc()
                self.root.after(0, lambda err_str=str(e_main_logic): messagebox.showerror("Erro Inesperado", f"Erro com {phone}: {err_str}", parent=self.root))
                self.update_status(f"Falha ({phone}): Erro inesperado.")
                if temp_client and temp_client.is_connected:
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
            else:
                self.update_status(f"Falha na conexão de {phone}.")
        except asyncio.TimeoutError:
            self.log_message(f"Timeout geral no processo de conexão de {phone} (300s) (Pyrogram).", 'error')
            self.update_status(f"Falha ({phone}): Timeout na conexão.")
            if future.cancel(): logging.debug(f"Future de conexão Pyrogram para {phone} cancelada devido a timeout.")
        except Exception as e_future:
            self.log_message(f"Erro ao obter resultado da future de conexão Pyrogram para {phone}: {e_future}", 'error')
            self.update_status(f"Falha grave ({phone}): {e_future}")
        finally:
            self.root.after(0, self.update_ui_elements_state)
            self.refresh_account_status_manager_if_open()

    def initialize_client_for_account_data(self, account_data_ref, connect_now=False):
        phone = account_data_ref['phone']
        api_id = account_data_ref['api_id']
        api_hash = account_data_ref['api_hash']
        session_name = phone
        try:
            api_id_int = int(api_id)
        except ValueError:
            self.log_message(f"API ID '{api_id}' para {phone} não é um número. Não foi possível inicializar cliente Pyrogram.", 'error')
            account_data_ref['client'] = None
            return
        new_client = Client(name=session_name, api_id=api_id_int, api_hash=api_hash, workdir=".")
        account_data_ref['client'] = new_client
        logging.debug(f"Objeto Client Pyrogram (re)inicializado para {phone} (Nome da sessão: {session_name}).")
        if connect_now:
             self.log_message(f"initialize_client_for_account_data (Pyrogram): connect_now=True. Conexão explícita é preferível.", "warning")

    def on_account_selection_change(self, event=None):
        selected_phone = self.account_var.get()
        if not selected_phone:
            self.client = None
            self.chats = []
            if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
            self.update_status("Nenhuma conta selecionada no combobox.")
            self.update_ui_elements_state()
            return
        account_data = self.get_selected_account_data_in_combobox()
        if account_data:
            self.log_message(f"Conta selecionada no combobox: {selected_phone} (Pyrogram)", "info")
            self.client = account_data.get('client')
            if not self.client:
                self.initialize_client_for_account_data(account_data)
                self.client = account_data.get('client')
            if self.client and self.client.is_connected:
                self.update_status(f"Conta {selected_phone} (Pyrogram) (selecionada) está Conectada.")
                self.load_chats_for_selected_account()
            else:
                self.update_status(f"Conta {selected_phone} (Pyrogram) (selecionada) está Desconectada.")
                self.chats = []
                if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
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
        if not selected_account_data or \
           not selected_account_data.get('client') or \
           not selected_account_data['client'].is_connected:
            self.log_message("Cliente Pyrogram da conta selecionada não conectado. Não é possível recarregar grupos.", 'error')
            messagebox.showerror("Erro", "Conecte a conta selecionada no combobox primeiro.")
            return
        self.load_chats_for_selected_account()

    def load_chats_for_selected_account(self):
        if not self.client or not self.client.is_connected:
            self.log_message("Tentativa de carregar chats sem cliente Pyrogram conectado (conta selecionada).", 'warning')
            self.update_ui_elements_state()
            return
        if self.chat_listbox: self.chat_listbox.delete(0, tk.END)
        self.chats = []
        self.update_status("Carregando grupos/canais da conta selecionada (Pyrogram)...")
        if self.reload_chats_button: self.reload_chats_button.config(state=tk.DISABLED)
        future = asyncio.run_coroutine_threadsafe(self._fetch_chats_async_for_selected_client(), self.loop)
        def on_chats_loaded_callback(ft):
            try:
                ft.result()
                self.log_message("Busca de chats da conta selecionada (Pyrogram) concluída.", 'debug')
                if not self.chats: self.update_status("Nenhum grupo/canal encontrado para a conta selecionada.")
                else: self.update_status(f"{len(self.chats)} grupos/canais carregados (conta selecionada).")
            except ConnectionError as e_conn_cb:
                self.log_message(f"Erro de conexão ao carregar chats (Pyrogram) (callback): {e_conn_cb}", 'error')
                self.update_status("Erro de conexão ao carregar chats.")
            except Exception as e:
                self.log_message(f"Erro final ao carregar chats (Pyrogram) (callback): {e}", 'error')
                self.update_status("Erro ao carregar grupos/canais.")
            finally:
                if hasattr(self, 'reload_chats_button') and self.reload_chats_button:
                     self.root.after(0, lambda: self.reload_chats_button.config(state=tk.NORMAL if self.client and self.client.is_connected else tk.DISABLED))
                self.root.after(0, self.update_ui_elements_state)
        future.add_done_callback(on_chats_loaded_callback)

    async def _fetch_chats_async_for_selected_client(self):
        self.log_message("Iniciando _fetch_chats_async para conta selecionada (Pyrogram)...", 'debug')
        try:
            if not self.client or not self.client.is_connected:
                self.log_message("Cliente Pyrogram (conta selecionada) desconectado antes de buscar chats.", 'error')
                raise ConnectionError("Cliente Pyrogram (conta selecionada) desconectado.")
            dialog_count = 0
            async for dialog in self.client.get_dialogs():
                dialog_count +=1
                if dialog_count % 50 == 0: self.update_status(f"Analisando diálogo {dialog_count} (conta selecionada)...")
                if dialog.chat and dialog.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
                    title = dialog.chat.title if dialog.chat.title else f"ID {dialog.chat.id}"
                    self.chats.append({'id': dialog.chat.id, 'title': title, 'entity': dialog.chat})
                    self.root.after(0, lambda t=title: self.chat_listbox.insert(tk.END, t) if self.chat_listbox else None)
            self.log_message(f"Busca de diálogos da conta selecionada (Pyrogram) finalizada. {len(self.chats)} grupos/canais encontrados.", 'info')
            if not self.chats: self.log_message("Nenhum grupo/canal qualificado encontrado para conta selecionada (Pyrogram).", 'warning')
        except Exception as e:
            self.log_message(f"Erro em _fetch_chats_async (conta selecionada) (Pyrogram): {e}", 'error')
            logging.error("Detalhes do erro em _fetch_chats_async (Pyrogram):", exc_info=True)

    def start_mass_sending_main(self):
        self.log_message("Iniciando start_mass_sending_main (Pyrogram)...", 'debug')
        if not self.client or not self.client.is_connected:
            self.log_message("Cliente Pyrogram (conta selecionada) não conectado.", 'error')
            messagebox.showerror("Erro", "Conecte a conta selecionada no combobox primeiro.")
            return
        message_text_content = self.message_text.get("1.0", tk.END).strip()
        selected_indices = self.chat_listbox.curselection()
        if not message_text_content:
            self.log_message("Mensagem vazia para envio em massa (MainWindow - Pyrogram).", 'error')
            messagebox.showerror("Erro", "A mensagem não pode estar vazia.")
            return
        if not selected_indices:
            self.log_message("Nenhum grupo selecionado para envio em massa (MainWindow - Pyrogram).", 'error')
            messagebox.showerror("Erro", "Selecione pelo menos um grupo/canal.")
            return
        try:
            interval_min = float(self.interval_entry.get())
            if interval_min < 0 : raise ValueError("Intervalo deve ser não-negativo")
        except ValueError:
            self.log_message("Intervalo inválido para envio em massa (MainWindow - Pyrogram).", 'error')
            messagebox.showerror("Erro", "Intervalo inválido. Insira um número não-negativo.")
            return
        interval_sec = interval_min * 60
        self.selected_chats_details_mass_main = [{'id': self.chats[i]['id'], 'title': self.chats[i]['title']} for i in selected_indices]
        self.running_mass_sending_main = True
        if self.start_sending_button: self.start_sending_button.config(state=tk.DISABLED)
        if self.stop_sending_button: self.stop_sending_button.config(state=tk.NORMAL)
        self.log_message(f"Iniciando envio em massa (MainWindow - Pyrogram) para {len(self.selected_chats_details_mass_main)} grupos/canais a cada {interval_min:.2f} minutos.", 'info')
        self.update_status("Iniciando envios em massa (MainWindow - Pyrogram)...")
        asyncio.run_coroutine_threadsafe(
            self._sender_coro_mass_main(message_text_content, self.selected_chats_details_mass_main, interval_sec),
            self.loop
        )
        messagebox.showinfo("Iniciado", f"Envio em massa (MainWindow - Pyrogram) iniciado para {len(self.selected_chats_details_mass_main)} grupos/canais.")

    async def _sender_coro_mass_main(self, message_to_send, chats_to_send_to_details, send_interval_seconds):
        while self.running_mass_sending_main:
            cycle_start_time = time.time()
            for chat_detail in chats_to_send_to_details:
                if not self.running_mass_sending_main: break
                chat_id = chat_detail['id']
                chat_title = chat_detail['title']
                self.update_status(f"Enviando (MainWindow - Pyrogram) para {chat_title}...")
                try:
                    if not self.client or not self.client.is_connected:
                        self.log_message("Cliente Pyrogram (conta selecionada) desconectou durante o envio (MainWindow). Parando.", "error")
                        self.running_mass_sending_main = False; break
                    await self.client.send_message(chat_id=chat_id, text=message_to_send)
                    self.log_message(f"Mensagem (MainWindow - Pyrogram) enviada para {chat_title}", "info")
                except FloodWait as e_flood:
                    self.log_message(f"FloodWait (MainWindow - Pyrogram) ao enviar para {chat_title}: {e_flood.value}s. Pausando.", "error")
                    self.update_status(f"FloodWait ({e_flood.value}s) para {chat_title}. Aguardando...")
                    await asyncio.sleep(e_flood.value + 5)
                except (UserPrivacyRestricted, UserKicked, UserBannedInChannel, ChatAdminRequired, UsernameNotOccupied, UserDeactivatedBan) as e_perm:
                    self.log_message(f"Erro de permissão (MainWindow - Pyrogram) ao enviar para {chat_title}: {type(e_perm).__name__}. Pulando.", "warning")
                except Exception as e_send:
                    self.log_message(f"Erro desconhecido (MainWindow - Pyrogram) ao enviar para {chat_title}: {e_send}", "error")
                    logging.error(f"Detalhe do erro em _sender_coro_mass_main (Pyrogram) para {chat_title}:", exc_info=True)
                if self.running_mass_sending_main and len(chats_to_send_to_details) > 1:
                    await asyncio.sleep(max(0.5, int(send_interval_seconds / len(chats_to_send_to_details) / 4)))
            if not self.running_mass_sending_main: break
            elapsed_time_cycle = time.time() - cycle_start_time
            wait_time = send_interval_seconds - elapsed_time_cycle
            if wait_time > 0:
                self.update_status(f"Aguardando próximo ciclo (MainWindow - Pyrogram) ({wait_time/60:.1f} min)...")
                wait_until = time.time() + wait_time
                while self.running_mass_sending_main and time.time() < wait_until: await asyncio.sleep(1)
            else:
                 self.update_status(f"Ciclo (MainWindow - Pyrogram) concluído. Preparando próximo...")
                 await asyncio.sleep(2)
        self.root.after(0, self._finalize_sending_mass_main)

    def _finalize_sending_mass_main(self):
        self.update_status("Envios em massa (MainWindow - Pyrogram) parados." if not self.running_mass_sending_main else "Envios em massa (MainWindow - Pyrogram) concluídos.")
        self.running_mass_sending_main = False
        self.update_ui_elements_state()

    def stop_mass_sending_main(self):
        if self.running_mass_sending_main:
            self.running_mass_sending_main = False
            self.log_message("Parando envio em massa de mensagens (MainWindow - Pyrogram)...", 'info')
        else:
            self.log_message("Nenhum processo de envio em massa (MainWindow - Pyrogram) ativo para parar.", "info")

def main():
    root = Window(themename="darkly")
    try:
        app = MainWindow(root)
        root.mainloop()
    except Exception as e:
        logging.critical(f"ERRO CRÍTICO AO INICIAR A APLICAÇÃO (Pyrogram): {e}", exc_info=True)
        try:
            err_root = tk.Tk()
            err_root.withdraw()
            messagebox.showerror("Erro Crítico na Inicialização",
                                 f"Falha grave ao iniciar a aplicação:\n\n{type(e).__name__}: {e}\n\n"
                                 "Verifique o console ou o arquivo de log para detalhes.")
            err_root.destroy()
        except Exception as e_msgbox:
            print(f"Não foi possível mostrar o messagebox de erro crítico: {e_msgbox}")
            print(f"ERRO CRÍTICO AO INICIAR A APLICAÇÃO (Pyrogram): {e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    log_file_path = "telepulse_pyrogram_errors.log"
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(filename)s:%(lineno)d - %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("pyrogram").setLevel(logging.WARNING)
        logging.info("--- Logging para arquivo configurado (Pyrogram). Nova sessão da aplicação iniciada. ---")
    except Exception as e_log_setup:
        print(f"Erro crítico ao configurar logging para arquivo: {e_log_setup}")
    main()
