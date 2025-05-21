import tkinter as tk
from tkinter import messagebox, scrolledtext
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants

from pyrogram.errors import ( # Adicionado Pyrogram errors
    FloodWait, UserPrivacyRestricted, UserChannelsTooMuch, UserKicked, UserBannedInChannel,
    ChatAdminRequired, UsersTooMuch, FreshChangePhoneForbidden, InviteHashExpired,
    RPCError, PeerFlood, UserAlreadyParticipant, InviteRequestSent,
    ChannelInvalid, ChannelPrivate, BadRequest, UserIdInvalid, UsernameNotOccupied # UserNotMutual REMOVIDO daqui
)
from pyrogram.enums import ChatMembersFilter, ChatType
import asyncio
import datetime
import json
import logging
import time

class MemberAdder:
    def __init__(self, main_app_ref):
        self.main_app = main_app_ref
        self.loop = self.main_app.loop

        self.members_to_add = []
        self.running_addition = False
        self.running_extraction_in_adder = False

        self.adder_window = None
        self.source_group_listbox_adder = None
        self.extract_button_adder = None
        self.members_to_add_text = None
        self.members_to_add_count_label = None
        self.target_group_listbox = None
        self.add_log_text = None
        self.add_interval_entry = None
        self.add_batch_limit_entry = None
        self.add_pause_duration_entry = None
        self.start_add_button = None
        self.stop_add_button = None

        self.setup_ui()
        self.update_members_list_from_manager()
        self._populate_source_group_listbox_adder()
        logging.debug("MemberAdder __init__ concluída.")

    def setup_ui(self):
        self.adder_window = Toplevel(master=self.main_app.root, title="Adicionar Membros a Grupos (Pyrogram)")
        self.adder_window.geometry("950x700")
        self.adder_window.protocol("WM_DELETE_WINDOW", self.on_close)

        main_frame = ttk.Frame(self.adder_window, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.adder_window.columnconfigure(0, weight=1)
        self.adder_window.rowconfigure(0, weight=1)

        top_info_frame = ttk.Frame(main_frame)
        top_info_frame.grid(row=0, column=0, columnspan=2, pady=5, sticky="ew")
        ttk.Label(top_info_frame, text="Operações usarão contas ATIVAS (configuradas no Gerenciador de Status).", bootstyle=ttk_constants.INFO).pack(pady=5)

        left_column_frame = ttk.Frame(main_frame)
        left_column_frame.grid(row=1, column=0, sticky="nsew", padx=(0,10))
        left_column_frame.rowconfigure(1, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)

        extraction_section_frame = ttk.LabelFrame(left_column_frame, text="Extrair Membros (Usar Conta da Janela Principal)", padding="10", bootstyle=ttk_constants.PRIMARY)
        extraction_section_frame.grid(row=0, column=0, pady=(0,10), sticky="ew")
        extraction_section_frame.columnconfigure(0, weight=1)

        ttk.Label(extraction_section_frame, text="Selecionar Grupo Fonte para Extrair:").grid(row=0, column=0, sticky="w", pady=(0,5))
        self.source_group_listbox_adder = tk.Listbox(extraction_section_frame, selectmode=tk.SINGLE, width=40, height=4, exportselection=False)
        self.source_group_listbox_adder.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
        self.extract_button_adder = ttk.Button(extraction_section_frame, text="Extrair Membros Deste Grupo", command=self.trigger_extract_members_in_adder, bootstyle=ttk_constants.SUCCESS)
        self.extract_button_adder.grid(row=2, column=0, padx=5, pady=5, sticky="ew")

        members_to_add_frame = ttk.LabelFrame(left_column_frame, text="Membros a Serem Adicionados (Extraídos ou Carregados)", padding="10", bootstyle=ttk_constants.INFO)
        members_to_add_frame.grid(row=1, column=0, pady=5, sticky="nsew")
        members_to_add_frame.columnconfigure(0, weight=1)
        members_to_add_frame.rowconfigure(0, weight=1)
        self.members_to_add_text = scrolledtext.ScrolledText(members_to_add_frame, width=40, height=10, state='disabled', wrap=tk.WORD)
        self.members_to_add_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.members_to_add_count_label = ttk.Label(members_to_add_frame, text="Total a Adicionar: 0", bootstyle="info-inverse", anchor="e")
        self.members_to_add_count_label.grid(row=1, column=0, padx=5, pady=2, sticky="ew")

        member_load_buttons_frame = ttk.Frame(left_column_frame)
        member_load_buttons_frame.grid(row=2, column=0, pady=5, sticky="ew")
        member_load_buttons_frame.columnconfigure(0, weight=1)
        member_load_buttons_frame.columnconfigure(1, weight=1)
        ttk.Button(member_load_buttons_frame, text="Recarregar Lista (do Gerenciador)", command=self.update_members_list_from_manager, bootstyle=ttk_constants.SECONDARY)\
            .grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        ttk.Button(member_load_buttons_frame, text="Abrir Gerenciador para Extrair", command=self.open_manager_to_extract, bootstyle=ttk_constants.INFO)\
            .grid(row=0, column=1, padx=2, pady=2, sticky="ew")

        right_column_frame = ttk.Frame(main_frame)
        right_column_frame.grid(row=1, column=1, sticky="nsew")
        right_column_frame.rowconfigure(2, weight=1)
        main_frame.columnconfigure(1, weight=1)

        target_group_frame = ttk.LabelFrame(right_column_frame, text="Grupo Alvo (da Conta Selecionada na Janela Principal)", padding="10", bootstyle=ttk_constants.INFO)
        target_group_frame.grid(row=0, column=0, pady=5, sticky="ew")
        target_group_frame.columnconfigure(0, weight=1)
        self.target_group_listbox = tk.Listbox(target_group_frame, selectmode=tk.SINGLE, width=40, height=6, exportselection=False)
        self.target_group_listbox.grid(row=0, column=0, padx=(5,0), pady=5, sticky="nsew")
        scrollbar_target = ttk.Scrollbar(target_group_frame, orient=tk.VERTICAL, command=self.target_group_listbox.yview, bootstyle="round-info")
        scrollbar_target.grid(row=0, column=1, sticky="ns", pady=5, padx=(0,5))
        self.target_group_listbox.config(yscrollcommand=scrollbar_target.set)
        self._update_target_group_listbox_from_main_app()

        ttk.Button(target_group_frame, text="Recarregar Grupos Alvo (da Janela Principal)", command=self.reload_main_app_target_chats, bootstyle=ttk_constants.SECONDARY)\
            .grid(row=1, column=0, columnspan=2, padx=2, pady=2, sticky="ew")

        add_config_frame = ttk.LabelFrame(right_column_frame, text="Configurações de Adição", padding="10", bootstyle=ttk_constants.INFO)
        add_config_frame.grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Label(add_config_frame, text="Intervalo (s):").grid(row=0, column=0, padx=(5,2), pady=2, sticky="w")
        self.add_interval_entry = ttk.Entry(add_config_frame, width=5)
        self.add_interval_entry.insert(0, "15")
        self.add_interval_entry.grid(row=0, column=1, padx=(0,5), pady=2, sticky="w")
        ttk.Label(add_config_frame, text="Lote:").grid(row=0, column=2, padx=(5,2), pady=2, sticky="w")
        self.add_batch_limit_entry = ttk.Entry(add_config_frame, width=5)
        self.add_batch_limit_entry.insert(0, "5")
        self.add_batch_limit_entry.grid(row=0, column=3, padx=(0,5), pady=2, sticky="w")
        ttk.Label(add_config_frame, text="Pausa (min):").grid(row=0, column=4, padx=(5,2), pady=2, sticky="w")
        self.add_pause_duration_entry = ttk.Entry(add_config_frame, width=5)
        self.add_pause_duration_entry.insert(0, "5")
        self.add_pause_duration_entry.grid(row=0, column=5, padx=(0,5), pady=2, sticky="w")

        add_buttons_frame = ttk.Frame(add_config_frame)
        add_buttons_frame.grid(row=1, column=0, columnspan=6, pady=5, sticky="ew")
        add_buttons_frame.columnconfigure(0,weight=1)
        add_buttons_frame.columnconfigure(1,weight=1)
        self.start_add_button = ttk.Button(add_buttons_frame, text="Iniciar Adição (Usar Contas ATIVAS)", command=self.trigger_add_members, bootstyle=ttk_constants.PRIMARY)
        self.start_add_button.grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        self.stop_add_button = ttk.Button(add_buttons_frame, text="Parar Adição", command=self.stop_member_addition, bootstyle=ttk_constants.DANGER, state=tk.DISABLED)
        self.stop_add_button.grid(row=0, column=1, padx=2, pady=2, sticky="ew")

        log_frame_adder = ttk.LabelFrame(right_column_frame, text="Log de Ações", padding="10", bootstyle=ttk_constants.INFO)
        log_frame_adder.grid(row=2, column=0, pady=5, sticky="nsew")
        log_frame_adder.columnconfigure(0, weight=1)
        log_frame_adder.rowconfigure(0, weight=1)
        self.add_log_text = scrolledtext.ScrolledText(log_frame_adder, width=40, height=10, state='disabled', wrap=tk.WORD)
        self.add_log_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")

        logging.debug("Janela de adição de membros configurada (Pyrogram).")
        self.adder_window.transient(self.main_app.root)
        self.adder_window.grab_set()

    def on_close(self):
        logging.debug("MemberAdder on_close chamado.")
        if self.running_addition:
            self.stop_member_addition()
        if self.running_extraction_in_adder:
            self.running_extraction_in_adder = False
            self.log_adder_message("Extração no Adder interrompida pelo fechamento da janela.", "info")
            if self.extract_button_adder and self.extract_button_adder.winfo_exists():
                self.extract_button_adder.config(state=tk.NORMAL)
        if self.main_app:
            self.main_app.member_adder_instance = None
        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.destroy()

    def reload_main_app_target_chats(self):
        self.log_adder_message("Solicitando recarregamento de grupos alvo da janela principal (Pyrogram)...", "debug")
        self.main_app.reload_chats_for_selected_account()
        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.after(1500, self._update_target_group_listbox_from_main_app)
            self.adder_window.after(1500, self._populate_source_group_listbox_adder)

    def _populate_source_group_listbox_adder(self):
        if self.source_group_listbox_adder and self.source_group_listbox_adder.winfo_exists():
            self.source_group_listbox_adder.delete(0, tk.END)
            if self.main_app.client and self.main_app.client.is_connected:
                for chat_detail in self.main_app.chats:
                    self.source_group_listbox_adder.insert(tk.END, chat_detail['title'])
                self.log_adder_message("Lista de grupos fonte para extração no Adder atualizada (Pyrogram).", "debug")
            else:
                self.log_adder_message("Conta da Janela Principal não conectada. Não é possível listar grupos para extração no Adder (Pyrogram).", "warning")

    def _update_target_group_listbox_from_main_app(self):
        if self.target_group_listbox and self.target_group_listbox.winfo_exists():
            self.target_group_listbox.delete(0, tk.END)
            if self.main_app.client and self.main_app.client.is_connected:
                for chat_detail in self.main_app.chats:
                    self.target_group_listbox.insert(tk.END, chat_detail['title'])
                self.log_adder_message("Lista de grupos alvo atualizada (Pyrogram).", "info")
            else:
                self.log_adder_message("Conta selecionada na Janela Principal não conectada. Grupos alvo podem estar desatualizados (Pyrogram).", "warning")
        else:
            self.log_adder_message("Widget target_group_listbox não encontrado para atualização (Pyrogram).", "warning")

    def update_members_list_from_manager(self):
        self.members_to_add = []
        if self.main_app.member_manager_instance and \
           hasattr(self.main_app.member_manager_instance, 'extracted_members'):
            self.members_to_add = list(self.main_app.member_manager_instance.extracted_members)
            self.log_adder_message(f"Lista de membros ({len(self.members_to_add)}) (re)carregada do Gerenciador.", "info")
        else:
            self.log_adder_message("Instância do Gerenciador de Membros não encontrada ou sem membros extraídos. Lista de adição vazia.", "warning")
        self.update_members_to_add_display()

    def open_manager_to_extract(self):
        self.log_adder_message("Solicitando abertura do Gerenciador de Membros para extração...", "info")
        self.main_app.open_member_manager()

    def update_members_to_add_display(self):
        if self.members_to_add_text and self.members_to_add_text.winfo_exists():
            self.members_to_add_text.configure(state='normal')
            self.members_to_add_text.delete("1.0", tk.END)
            for uid, uname, sphone in self.members_to_add:
                self.members_to_add_text.insert(tk.END, f"{uname} (ID: {uid}, Fonte: {sphone})\n")
            self.members_to_add_text.configure(state='disabled')
        if self.members_to_add_count_label and self.members_to_add_count_label.winfo_exists():
            self.members_to_add_count_label.config(text=f"Total a Adicionar: {len(self.members_to_add)}")

    def log_adder_message(self, message, level='info'):
        self.main_app.log_message(f"[MemberAdder] {message}", level)
        timestamp_str = datetime.datetime.now().strftime("%H:%M:%S")
        ui_log_msg = f"[{timestamp_str}] {message}"
        is_adder_log = True
        if "[MemberManager]" in message or "[AccountStatusManager]" in message:
            is_adder_log = False
        if is_adder_log:
            if "Membro adicionado @" in message:
                parts = message.split("adicionado ")
                if len(parts) > 1:
                    target_info = parts[1].split(" ao ")[0]
                    group_info = parts[1].split(" ao ")[1] if " ao " in parts[1] else ""
                    ui_log_msg = f"[{timestamp_str}] {target_info} -> {group_info} - SUCESSO ✅"
            elif "Erro ao adicionar @" in message:
                parts = message.split("adicionar ")
                if len(parts) > 1:
                    target_info_parts = parts[1].split(":")
                    target_info = target_info_parts[0].split(" ao ")[0]
                    error_detail = target_info_parts[1].strip() if len(target_info_parts) > 1 else "Erro"
                    ui_log_msg = f"[{timestamp_str}] {target_info} - ERRO ❌ ({error_detail})"
        def _log_to_member_adder_ui():
            try:
                if hasattr(self, 'adder_window') and self.adder_window and \
                   hasattr(self, 'add_log_text') and self.add_log_text and \
                   self.adder_window.winfo_exists() and self.add_log_text.winfo_exists():
                    self.add_log_text.configure(state='normal')
                    self.add_log_text.insert(tk.END, f"{ui_log_msg}\n")
                    self.add_log_text.see(tk.END)
                    self.add_log_text.configure(state='disabled')
            except Exception as e_ui_log:
                logging.error(f"Erro ao logar na UI do MemberAdder: {e_ui_log}", exc_info=True)
        if hasattr(self, 'adder_window') and self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.after(0, _log_to_member_adder_ui)
        else:
            logging.debug(f"MemberAdder UI não pronta, logando no console: {ui_log_msg}")

    def _get_operable_clients_for_adder_action(self):
        operable_accounts_data = self.main_app.get_operable_accounts()
        clients_for_action = []
        if not operable_accounts_data:
            self.log_adder_message("Nenhuma conta ATIVA encontrada para a ação de adição (Pyrogram).", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Nenhuma Conta Ativa", "Nenhuma conta está marcada como ATIVA no Gerenciador de Status.", parent=self.adder_window)
            return []
        for acc_data in operable_accounts_data:
            client_obj = acc_data.get('client')
            if not client_obj:
                self.main_app.initialize_client_for_account_data(acc_data)
                client_obj = acc_data.get('client')
            if client_obj:
                clients_for_action.append(acc_data)
            else:
                self.log_adder_message(f"Não foi possível obter/inicializar cliente Pyrogram para {acc_data.get('phone')} em MemberAdder. Pulando.", "warning")
        if not clients_for_action:
            self.log_adder_message("Nenhum cliente Pyrogram utilizável encontrado para adição após verificação/inicialização.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                 messagebox.showerror("Nenhum Cliente Utilizável", "Não foi possível preparar nenhum cliente para adição. Verifique status e conexão das contas ATIVAS.", parent=self.adder_window)
        return clients_for_action

    def trigger_extract_members_in_adder(self):
        if self.running_extraction_in_adder:
            self.log_adder_message("Extração já está em andamento nesta janela (Pyrogram).", "warning")
            return
        selected_indices_source_group = self.source_group_listbox_adder.curselection()
        if not selected_indices_source_group:
            self.log_adder_message("Nenhum grupo fonte selecionado para extração no Adder (Pyrogram).", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Selecione um grupo fonte da lista para extrair.", parent=self.adder_window)
            return
        source_group_index_in_main_chats = selected_indices_source_group[0]
        try:
            source_chat_detail = self.main_app.chats[source_group_index_in_main_chats]
            source_chat_identifier = source_chat_detail['id']
            source_chat_title = source_chat_detail['title']
        except (IndexError, KeyError) as e:
            self.log_adder_message(f"Erro ao obter detalhes do grupo fonte para extração no Adder (Pyrogram) ({e}).", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Grupo fonte inválido. Recarregue os grupos na Janela Principal.", parent=self.adder_window)
            return
        client_for_extraction = self.main_app.client
        phone_of_extraction_client = self.main_app.account_var.get()
        if not (client_for_extraction and client_for_extraction.is_connected):
            self.log_adder_message(f"A conta selecionada na Janela Principal ({phone_of_extraction_client}) não está conectada (Pyrogram). Conecte-a primeiro.", "error")
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Conta Desconectada", f"A conta {phone_of_extraction_client} (selecionada na Janela Principal) precisa estar conectada para extrair membros.", parent=self.adder_window)
            return
        self.running_extraction_in_adder = True
        self.log_adder_message(f"Iniciando extração de '{source_chat_title}' (ID: {source_chat_identifier}) usando a conta {phone_of_extraction_client} (a partir do Adder - Pyrogram)...", 'info')
        if self.extract_button_adder and self.extract_button_adder.winfo_exists():
            self.extract_button_adder.config(state=tk.DISABLED)
        self.members_to_add = []
        self.update_members_to_add_display()
        client_data_for_extraction = {
            'client': client_for_extraction,
            'phone': phone_of_extraction_client
        }
        asyncio.run_coroutine_threadsafe(
            self._internal_extract_members_core(source_chat_identifier, source_chat_title, client_data_for_extraction),
            self.loop
        )

    async def _internal_extract_members_core(self, source_chat_id, source_chat_title, client_data_dict):
        self.log_adder_message(f"Extração interna (Adder - Pyrogram) iniciada para '{source_chat_title}'...", "debug")
        temp_extracted_members = []
        globally_extracted_user_ids_temp = set()
        client = client_data_dict.get('client')
        phone_of_client = client_data_dict.get('phone')
        current_account_extracted_count = 0
        try:
            async for member in client.get_chat_members(source_chat_id):
                if not self.running_extraction_in_adder: break
                user = member.user
                if user.is_bot or not user.username:
                    continue
                if user.id not in globally_extracted_user_ids_temp:
                    globally_extracted_user_ids_temp.add(user.id)
                    member_tuple = (user.id, f"@{user.username}", phone_of_client)
                    temp_extracted_members.append(member_tuple)
                    current_account_extracted_count += 1
                    if current_account_extracted_count % 20 == 0:
                        self.members_to_add = list(temp_extracted_members)
                        if self.adder_window and self.adder_window.winfo_exists():
                            self.adder_window.after(0, self.update_members_to_add_display)
            if not self.running_extraction_in_adder:
                 self.log_adder_message(f"Extração (Adder - Pyrogram) de '{source_chat_title}' interrompida.", "info")
        except FloodWait as e_flood:
            self.log_adder_message(f"Conta {phone_of_client} (Adder): FloodWait ({e_flood.value}s) ao extrair de '{source_chat_title}' (Pyrogram).", 'error')
            await asyncio.sleep(e_flood.value + 5)
        except (ChatAdminRequired, ChannelInvalid, ChannelPrivate, BadRequest) as e_perm_rpc:
             self.log_adder_message(f"Conta {phone_of_client} (Adder): Erro de permissão/entidade ao extrair de '{source_chat_title}' (Pyrogram) ({type(e_perm_rpc).__name__}: {e_perm_rpc}).", 'warning')
        except Exception as e:
            self.log_adder_message(f"Conta {phone_of_client} (Adder): Erro ao extrair de '{source_chat_title}' (Pyrogram) - {type(e).__name__}: {e}", 'error')
            logging.error(f"Detalhe do erro de extração no Adder (Pyrogram) com {phone_of_client} para '{source_chat_title}':", exc_info=True)
        self.members_to_add = list(temp_extracted_members)
        def final_ui_update():
            if not (self.adder_window and self.adder_window.winfo_exists()): return
            self.update_members_to_add_display()
            self.log_adder_message(f"Extração (Adder - Pyrogram) concluída. {len(self.members_to_add)} membros carregados para adição.", 'info')
            if self.extract_button_adder and self.extract_button_adder.winfo_exists():
                self.extract_button_adder.config(state=tk.NORMAL)
            self.running_extraction_in_adder = False
        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.after(0, final_ui_update)
        else:
            self.running_extraction_in_adder = False

    def trigger_add_members(self):
        if self.running_addition:
            self.log_adder_message("Adição já está em andamento (Pyrogram).", "warning")
            return
        if not self.members_to_add:
            self.log_adder_message("Nenhum membro na lista para adicionar (Pyrogram).", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Não há membros na lista para adicionar. Extraia membros ou recarregue do Gerenciador.", parent=self.adder_window)
            return
        selected_target_indices = self.target_group_listbox.curselection()
        if not selected_target_indices:
            self.log_adder_message("Nenhum grupo alvo selecionado (Pyrogram).", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Selecione um grupo alvo para adicionar os membros.", parent=self.adder_window)
            return
        target_group_index_in_main_chats = selected_target_indices[0]
        try:
            target_chat_detail = self.main_app.chats[target_group_index_in_main_chats]
            target_chat_id = target_chat_detail['id']
            target_chat_title = target_chat_detail['title']
        except IndexError:
            self.log_adder_message("Erro ao obter detalhes do grupo alvo (Pyrogram). Recarregue.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Grupo alvo inválido. Tente recarregar a lista de grupos.", parent=self.adder_window)
            return
        except KeyError:
            self.log_adder_message("Detalhes do grupo alvo não encontrados (Pyrogram). Recarregue.", "error")
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Informações do grupo alvo ausentes. Recarregue.", parent=self.adder_window)
            return
        clients_data_to_use = self._get_operable_clients_for_adder_action()
        if not clients_data_to_use:
            self.log_adder_message("Nenhuma conta ATIVA configurada ou utilizável para adição (Pyrogram).", "warning")
            return
        try:
            interval_s = float(self.add_interval_entry.get())
            batch_lim = int(self.add_batch_limit_entry.get())
            pause_m = float(self.add_pause_duration_entry.get())
            if interval_s < 0 or batch_lim <= 0 or pause_m < 0:
                raise ValueError("Valores de configuração de adição inválidos.")
        except ValueError as ve:
            self.log_adder_message(f"Configuração de adição inválida (Pyrogram): {ve}", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro de Configuração", "Verifique os valores de intervalo, lote e pausa.", parent=self.adder_window)
            return
        pause_s = pause_m * 60
        self.running_addition = True
        if self.start_add_button and self.start_add_button.winfo_exists():
            self.start_add_button.config(state=tk.DISABLED)
        if self.stop_add_button and self.stop_add_button.winfo_exists():
            self.stop_add_button.config(state=tk.NORMAL)
        if self.extract_button_adder and self.extract_button_adder.winfo_exists():
            self.extract_button_adder.config(state=tk.DISABLED)
        self.log_adder_message(f"Iniciando adição (Pyrogram) de {len(self.members_to_add)} membros a '{target_chat_title}'. Contas ATIVAS: {len(clients_data_to_use)}. Intervalo: {interval_s}s, Lote: {batch_lim}, Pausa: {pause_m}min.", 'info')
        asyncio.run_coroutine_threadsafe(
            self._add_members_core(target_chat_id, target_chat_title, list(self.members_to_add), interval_s, batch_lim, pause_s, clients_data_to_use),
            self.loop
        )

    async def _add_members_core(self, target_chat_id_param, target_chat_title_str, members_data_list, interval, batch_limit, pause_duration_sec, clients_data_list_param):
        added_count_total = 0
        error_count_total = 0
        client_idx = 0
        active_clients_in_run = list(clients_data_list_param)
        if not active_clients_in_run:
            self.log_adder_message("Nenhuma conta cliente Pyrogram conectada e ATIVA fornecida para _add_members_core.", "error")
            if self.adder_window and self.adder_window.winfo_exists():
                self.adder_window.after(0, self._finalize_member_addition, 0, len(members_data_list))
            return
        processed_member_ids_this_run = set()
        member_index_loop = 0
        while member_index_loop < len(members_data_list) and self.running_addition:
            if not self.running_addition:
                self.log_adder_message("Adição de membros interrompida (loop principal - Pyrogram).", "info")
                break
            user_id_to_add, username_to_add, _ = members_data_list[member_index_loop]
            if user_id_to_add in processed_member_ids_this_run:
                member_index_loop += 1
                continue
            if not active_clients_in_run:
                self.log_adder_message("Nenhuma conta ativa restante para continuar a adição (Pyrogram). Interrompendo.", "warning")
                break
            current_client_data = active_clients_in_run[client_idx % len(active_clients_in_run)]
            client = current_client_data.get('client')
            client_phone = current_client_data.get('phone')
            if not client:
                self.log_adder_message(f"Conta {client_phone}: Cliente Pyrogram não encontrado no loop. Pulando {username_to_add}.", "error")
                error_count_total +=1
                processed_member_ids_this_run.add(user_id_to_add)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                continue
            if not client.is_connected:
                self.log_adder_message(f"Conta {client_phone}: Não está conectada (Pyrogram). Tentando reconectar para adicionar {username_to_add}...", "info")
                try:
                    await client.connect()
                    if not client.is_initialized:
                        self.log_adder_message(f"Conta {client_phone}: Falha na autorização ao reconectar (Pyrogram). Tentando próxima.", "error")
                        active_clients_in_run.pop(client_idx % len(active_clients_in_run))
                        client_idx = 0
                        if not active_clients_in_run: break
                        continue
                except Exception as e_conn:
                    self.log_adder_message(f"Conta {client_phone}: Erro ao reconectar (Pyrogram) - {e_conn}. Tentando próxima.", "error")
                    active_clients_in_run.pop(client_idx % len(active_clients_in_run))
                    client_idx = 0
                    if not active_clients_in_run: break
                    continue
            self.log_adder_message(f"Conta {client_phone}: Tentando adicionar {username_to_add} (ID: {user_id_to_add}) a '{target_chat_title_str}' (Pyrogram)...", 'debug')
            try:
                await client.add_chat_members(chat_id=target_chat_id_param, user_ids=user_id_to_add)
                self.log_adder_message(f"Conta {client_phone}: Membro {username_to_add} adicionado a '{target_chat_title_str}' (Pyrogram).", 'info')
                added_count_total += 1
                processed_member_ids_this_run.add(user_id_to_add)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                if added_count_total > 0 and added_count_total % batch_limit == 0 and member_index_loop < len(members_data_list):
                    if pause_duration_sec > 0:
                        self.log_adder_message(f"Lote de {added_count_total} (total) / {batch_limit} (limite) atingido (Pyrogram). Pausando por {pause_duration_sec/60:.1f} minutos...", 'info')
                        pause_until = time.time() + pause_duration_sec
                        while self.running_addition and time.time() < pause_until: await asyncio.sleep(1)
                        if not self.running_addition:
                            self.log_adder_message("Adição interrompida durante pausa do lote (Pyrogram).", "info"); break
                if self.running_addition and interval > 0 and member_index_loop < len(members_data_list):
                    await asyncio.sleep(interval)
            except (FloodWait, PeerFlood) as e_flood:
                self.log_adder_message(f"Conta {client_phone}: {type(e_flood).__name__} ({getattr(e_flood, 'value', 'N/A')}s) ao adicionar {username_to_add}. Desativando e removendo da sessão.", 'error')
                error_count_total +=1
                processed_member_ids_this_run.add(user_id_to_add)
                member_index_loop +=1
                account_to_deactivate = self.main_app.get_account_by_phone(client_phone)
                if account_to_deactivate:
                    account_to_deactivate['app_status'] = 'INATIVO'
                    self.main_app.save_accounts()
                    self.main_app.refresh_account_status_manager_if_open()
                active_clients_in_run = [acc for acc in active_clients_in_run if acc.get('phone') != client_phone]
                client_idx = 0
                if hasattr(e_flood, 'value'): await asyncio.sleep(e_flood.value + 5)
                else: await asyncio.sleep(60)
                if not active_clients_in_run:
                    self.log_adder_message("Todas as contas ativas foram desativadas (Pyrogram). Interrompendo adição.", "critical"); break
            # UserNotMutual REMOVIDO DAQUI
            except (UserPrivacyRestricted, UsersTooMuch, UserChannelsTooMuch, ChatAdminRequired,
                      UserKicked, UserBannedInChannel, UserAlreadyParticipant, InviteRequestSent,
                      UserIdInvalid, UsernameNotOccupied, BadRequest
                     ) as e_specific:
                 self.log_adder_message(f"Conta {client_phone}: Erro específico ao adicionar {username_to_add} (Pyrogram) - {type(e_specific).__name__}: {e_specific}. Pulando este membro.", 'warning')
                 error_count_total +=1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                 client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
            except Exception as e_add:
                self.log_adder_message(f"Conta {client_phone}: Erro desconhecido ao adicionar {username_to_add} a '{target_chat_title_str}' (Pyrogram): {e_add}", 'error')
                logging.error(f"Detalhe do erro de adição (Pyrogram) com {client_phone} para {username_to_add}:", exc_info=True)
                error_count_total += 1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.after(0, self._finalize_member_addition, added_count_total, error_count_total)
        else:
            self._finalize_member_addition_logic(added_count_total, error_count_total)

    def _finalize_member_addition_logic(self, added_count, error_count):
        self.running_addition = False
        self.log_adder_message(f"Adição de membros finalizada (Pyrogram). Adicionados: {added_count}, Erros/Falhas: {error_count}.", 'info')

    def _finalize_member_addition(self, added_count, error_count):
        self._finalize_member_addition_logic(added_count, error_count)
        if not (self.adder_window and self.adder_window.winfo_exists()):
            return
        if self.start_add_button and self.start_add_button.winfo_exists():
            self.start_add_button.config(state=tk.NORMAL)
        if self.stop_add_button and self.stop_add_button.winfo_exists():
            self.stop_add_button.config(state=tk.DISABLED)
        if self.extract_button_adder and self.extract_button_adder.winfo_exists():
            self.extract_button_adder.config(state=tk.NORMAL)
        if self.adder_window and self.adder_window.winfo_exists():
            messagebox.showinfo("Adição Concluída", f"Processo de adição finalizado (Pyrogram).\nAdicionados: {added_count}\nErros/Falhas: {error_count}", parent=self.adder_window)

    def stop_member_addition(self):
        if self.running_addition:
            self.running_addition = False
            self.log_adder_message("Solicitando parada da adição de membros (Pyrogram)...", 'info')
        else:
            self.log_adder_message("Nenhum processo de adição ativo para parar (Pyrogram).", "info")
