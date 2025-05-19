import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError, UserChannelsTooMuchError, 
    UserKickedError, UserBannedInChannelError, ChatAdminRequiredError, 
    UsernameNotOccupiedError
)
import asyncio
import datetime
import json
import logging
import time

class MemberManager:
    def __init__(self, main_app_ref):
        self.main_app = main_app_ref
        self.loop = self.main_app.loop
        self.extracted_members = []
        self.running_extraction = False
        self.running_mass_sending = False
        self.member_window = None
        self.member_list_text = None
        self.member_count_label = None
        self.group_listbox = None
        self.extract_button = None
        self.save_button = None
        self.load_button = None
        self.message_text = None
        self.interval_entry = None
        self.batch_limit_entry = None
        self.pause_duration_entry = None
        self.start_sending_button = None
        self.stop_sending_button = None
        self.log_text = None
        self.setup_ui()
        logging.debug("MemberManager __init__ concluída.")

    def setup_ui(self):
        self.member_window = Toplevel(master=self.main_app.root, title="Gerenciador de Membros")
        self.member_window.geometry("900x700")
        self.member_window.protocol("WM_DELETE_WINDOW", self.on_close)
        main_frame = ttk.Frame(self.member_window, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.member_window.columnconfigure(0, weight=1)
        self.member_window.rowconfigure(0, weight=1)
        top_info_frame = ttk.Frame(main_frame)
        top_info_frame.grid(row=0, column=0, columnspan=2, pady=5, sticky="ew")
        ttk.Label(top_info_frame, text="Extração e envio usarão contas ATIVAS (configuradas no Gerenciador de Status).", bootstyle=ttk_constants.INFO).pack(pady=5)
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky="nsew", padx=(0,5))
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        left_frame.rowconfigure(0, weight=1)
        members_frame = ttk.LabelFrame(left_frame, text="Membros Extraídos", padding="10", bootstyle=ttk_constants.INFO)
        members_frame.grid(row=0, column=0, pady=5, sticky="nsew")
        members_frame.columnconfigure(0, weight=1)
        members_frame.rowconfigure(0, weight=1)
        self.member_list_text = scrolledtext.ScrolledText(members_frame, width=40, height=15, state='disabled', wrap=tk.WORD)
        self.member_list_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.member_count_label = ttk.Label(members_frame, text="Total Extraído: 0", bootstyle="info-inverse", anchor="e")
        self.member_count_label.grid(row=1, column=0, padx=5, pady=2, sticky="ew")
        groups_frame = ttk.LabelFrame(left_frame, text="Grupos (Contas ATIVAS)", padding="10", bootstyle=ttk_constants.INFO)
        groups_frame.grid(row=1, column=0, pady=5, sticky="nsew")
        groups_frame.columnconfigure(0, weight=1)
        self.group_listbox = tk.Listbox(groups_frame, selectmode=tk.MULTIPLE, width=40, height=6, exportselection=False)
        self.group_listbox.grid(row=0, column=0, padx=(5,0), pady=5, sticky="nsew")
        scrollbar = ttk.Scrollbar(groups_frame, orient=tk.VERTICAL, command=self.group_listbox.yview, bootstyle="round-info")
        scrollbar.grid(row=0, column=1, sticky="ns", pady=5, padx=(0,5))
        self.group_listbox.config(yscrollcommand=scrollbar.set)
        self.group_listbox.bind('<<ListboxSelect>>', self.on_group_selection)
        self.extract_button = ttk.Button(groups_frame, text="Extrair Membros dos Grupos Selecionados", command=self.extract_members_trigger, bootstyle=ttk_constants.PRIMARY)
        self.extract_button.grid(row=1, column=0, columnspan=2, padx=5, pady=2, sticky="ew")
        file_buttons_frame = ttk.Frame(left_frame)
        file_buttons_frame.grid(row=2, column=0, pady=5, sticky="ew")
        file_buttons_frame.columnconfigure(0, weight=1)
        file_buttons_frame.columnconfigure(1, weight=1)
        self.save_button = ttk.Button(file_buttons_frame, text="Salvar Lista de Membros", command=self.save_members_to_file, bootstyle=ttk_constants.SECONDARY)
        self.save_button.grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        self.load_button = ttk.Button(file_buttons_frame, text="Carregar Lista de Membros", command=self.load_members_from_file, bootstyle=ttk_constants.SECONDARY)
        self.load_button.grid(row=0, column=1, padx=2, pady=2, sticky="ew")
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky="nsew", padx=(5,0))
        main_frame.columnconfigure(1, weight=1)
        right_frame.rowconfigure(2, weight=1)
        message_frame = ttk.LabelFrame(right_frame, text="Mensagem em Massa (Mensagens Privadas)", padding="10", bootstyle=ttk_constants.INFO)
        message_frame.grid(row=0, column=0, pady=5, sticky="ew")
        message_frame.columnconfigure(0, weight=1)
        self.message_text = scrolledtext.ScrolledText(message_frame, width=40, height=8, wrap=tk.WORD)
        self.message_text.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        # Adicionar binding para detectar mudanças no texto da mensagem
        self.message_text.bind('<KeyRelease>', self.on_message_text_change)
        interval_frame = ttk.Frame(message_frame)
        interval_frame.grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Label(interval_frame, text="Intervalo (s):").pack(side=tk.LEFT, padx=5, pady=5)
        self.interval_entry = ttk.Entry(interval_frame, width=5)
        self.interval_entry.insert(0, "15")
        self.interval_entry.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Label(interval_frame, text="Lote:").pack(side=tk.LEFT, padx=5, pady=5)
        self.batch_limit_entry = ttk.Entry(interval_frame, width=5)
        self.batch_limit_entry.insert(0, "5")
        self.batch_limit_entry.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Label(interval_frame, text="Pausa (min):").pack(side=tk.LEFT, padx=5, pady=5)
        self.pause_duration_entry = ttk.Entry(interval_frame, width=5)
        self.pause_duration_entry.insert(0, "5")
        self.pause_duration_entry.pack(side=tk.LEFT, padx=5, pady=5)
        send_buttons_frame = ttk.Frame(message_frame)
        send_buttons_frame.grid(row=2, column=0, pady=5, sticky="ew")
        send_buttons_frame.columnconfigure(0, weight=1)
        send_buttons_frame.columnconfigure(1, weight=1)
        self.start_sending_button = ttk.Button(send_buttons_frame, text="Iniciar Envio em Massa", command=self.start_mass_sending, bootstyle=ttk_constants.PRIMARY)
        self.start_sending_button.grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        self.stop_sending_button = ttk.Button(send_buttons_frame, text="Parar Envio", command=self.stop_mass_sending, bootstyle=ttk_constants.DANGER, state=tk.DISABLED)
        self.stop_sending_button.grid(row=0, column=1, padx=2, pady=2, sticky="ew")
        log_frame = ttk.LabelFrame(right_frame, text="Log de Extração/Envio", padding="10", bootstyle=ttk_constants.INFO)
        log_frame.grid(row=2, column=0, pady=5, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = scrolledtext.ScrolledText(log_frame, width=40, height=10, state='disabled', wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.update_group_listbox_from_main_app()
        self.update_ui_elements_state()
        logging.debug("Janela de gerenciamento de membros configurada.")
        self.member_window.transient(self.main_app.root)
        self.member_window.grab_set()

    def on_group_selection(self, event=None):
        """Chamado quando a seleção no group_listbox muda."""
        selected_indices = self.group_listbox.curselection()
        self.log_message(f"Seleção de grupos alterada: {selected_indices}", "debug")
        self.update_ui_elements_state()

    def on_message_text_change(self, event=None):
        """Chamado quando o texto no message_text muda."""
        self.log_message(f"Texto da mensagem alterado: {len(self.message_text.get('1.0', tk.END).strip())} caracteres", "debug")
        self.update_ui_elements_state()

    def update_ui_elements_state(self):
        has_members = bool(self.extracted_members)
        has_selected_groups = bool(self.group_listbox.curselection())
        message_content = self.message_text.get("1.0", tk.END).strip()
        can_extract = has_selected_groups and not self.running_extraction
        can_send = has_members and not self.running_mass_sending and message_content
        self.log_message(f"Atualizando estado da UI: has_members={has_members}, running_mass_sending={self.running_mass_sending}, message_content_len={len(message_content)}, can_send={can_send}, has_selected_groups={has_selected_groups}, running_extraction={self.running_extraction}, can_extract={can_extract}", "debug")
        self.extract_button.config(state=tk.NORMAL if can_extract else tk.DISABLED)
        self.save_button.config(state=tk.NORMAL if has_members else tk.DISABLED)
        self.load_button.config(state=tk.NORMAL)
        self.start_sending_button.config(state=tk.NORMAL if can_send else tk.DISABLED)
        self.stop_sending_button.config(state=tk.NORMAL if self.running_mass_sending else tk.DISABLED)

    def on_close(self):
        logging.debug("MemberManager on_close chamado.")
        if self.running_extraction or self.running_mass_sending:
            self.stop_extraction()
            self.stop_mass_sending()
        self.running_extraction = False
        self.running_mass_sending = False  # Garante que o estado seja redefinido
        if self.main_app:
            self.main_app.member_manager_instance = None
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.destroy()

    def update_group_listbox_from_main_app(self):
        if self.group_listbox and self.group_listbox.winfo_exists():
            self.group_listbox.delete(0, tk.END)
            accounts_data = self.main_app.get_operable_accounts()
            self.log_message(f"Atualizando group_listbox: {len(accounts_data)} contas ativas encontradas.", "debug")
            if accounts_data:
                for acc_data in accounts_data:
                    client = acc_data.get('client')
                    phone = acc_data.get('phone', 'N/A')
                    if client and client.is_connected():
                        chats = self.main_app.chats if acc_data.get('phone') == self.main_app.account_var.get() else []
                        self.log_message(f"Conta {phone}: {len(chats)} grupos encontrados.", "debug")
                        for chat in chats:
                            self.group_listbox.insert(tk.END, f"{chat['title']} ({phone})")
                    else:
                        self.log_message(f"Conta {phone} não conectada. Grupos não listados.", "warning")
            else:
                self.log_message("Nenhuma conta ATIVA para listar grupos.", "warning")
        self.update_ui_elements_state()

    def log_message(self, message, level='info'):
        self.main_app.log_message(f"[MemberManager] {message}", level)
        timestamp_str = datetime.datetime.now().strftime("%H:%M:%S")
        ui_log_msg = f"[{timestamp_str}] {message}"
        is_member_manager_log = True
        if "[MemberAdder]" in message or "[AccountStatusManager]" in message:
            is_member_manager_log = False
        if is_member_manager_log:
            if "Membro extraído" in message:
                parts = message.split("extraído ")
                if len(parts) > 1:
                    username = parts[1].split(" (")[0]
                    group_name = parts[1].split(" de ")[1].split(" (")[0] if " de " in parts[1] else ""
                    ui_log_msg = f"[{timestamp_str}] {username} extraído de {group_name} - SUCESSO ✅"
            elif "Erro ao extrair" in message:
                parts = message.split("extrair ")
                if len(parts) > 1:
                    group_name = parts[1].split(":")[0]
                    error_detail = parts[1].split(":")[1].strip() if ":" in parts[1] else "Erro desconhecido"
                    ui_log_msg = f"[{timestamp_str}] {group_name} - ERRO ❌ ({error_detail})"
            elif "Mensagem enviada para" in message:
                parts = message.split("enviada para ")
                if len(parts) > 1:
                    username = parts[1].split(" (")[0]
                    ui_log_msg = f"[{timestamp_str}] {username} - SUCESSO ✅"
            elif "Erro ao enviar para" in message:
                parts = message.split("enviar para ")
                if len(parts) > 1:
                    username = parts[1].split(":")[0]
                    error_detail = parts[1].split(":")[1].strip() if ":" in parts[1] else "Erro desconhecido"
                    ui_log_msg = f"[{timestamp_str}] {username} - ERRO ❌ ({error_detail})"
        def _log_to_member_manager_ui():
            try:
                if self.log_text and self.log_text.winfo_exists():
                    self.log_text.configure(state='normal')
                    self.log_text.insert(tk.END, f"{ui_log_msg}\n")
                    self.log_text.see(tk.END)
                    self.log_text.configure(state='disabled')
            except Exception as e_ui_log:
                logging.error(f"Erro ao logar na UI do MemberManager: {e_ui_log}", exc_info=True)
        if hasattr(self.member_window, 'after') and self.member_window.winfo_exists():
            self.member_window.after(0, _log_to_member_manager_ui)

    def save_members_to_file(self):
        if not self.extracted_members:
            self.log_message("Nenhuma lista de membros para salvar.", "warning")
            messagebox.showwarning("Sem Membros", "Nenhuma lista de membros extraída para salvar.", parent=self.member_window)
            return
        file_path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")], parent=self.member_window)
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.extracted_members, f, ensure_ascii=False, indent=2)
                self.log_message(f"Lista de membros salva em {file_path}.", "info")
                messagebox.showinfo("Sucesso", f"Lista de membros salva em {file_path}.", parent=self.member_window)
            except Exception as e:
                self.log_message(f"Erro ao salvar lista de membros: {e}", "error")
                messagebox.showerror("Erro", f"Falha ao salvar a lista: {e}", parent=self.member_window)

    def load_members_from_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json"), ("All files", "*.*")], parent=self.member_window)
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    loaded_members = json.load(f)
                if not isinstance(loaded_members, list) or not all(isinstance(m, (list, tuple)) and len(m) == 3 for m in loaded_members):
                    raise ValueError("Formato de arquivo inválido. Esperado: lista de [user_id, username, source_phone].")
                self.extracted_members = [(int(m[0]), m[1], m[2]) for m in loaded_members]
                self.update_member_list_display()
                self.log_message(f"Lista de {len(self.extracted_members)} membros carregada de {file_path}.", "info")
                messagebox.showinfo("Sucesso", f"Carregados {len(self.extracted_members)} membros de {file_path}.", parent=self.member_window)
            except Exception as e:
                self.log_message(f"Erro ao carregar lista de membros: {e}", "error")
                messagebox.showerror("Erro", f"Falha ao carregar a lista: {e}", parent=self.member_window)

    def update_member_list_display(self):
        if self.member_list_text and self.member_list_text.winfo_exists():
            self.member_list_text.configure(state='normal')
            self.member_list_text.delete("1.0", tk.END)
            for uid, uname, sphone in self.extracted_members:
                self.member_list_text.insert(tk.END, f"{uname} (ID: {uid}, Fonte: {sphone})\n")
            self.member_list_text.configure(state='disabled')
        if self.member_count_label and self.member_count_label.winfo_exists():
            self.member_count_label.config(text=f"Total Extraído: {len(self.extracted_members)}")
        self.update_ui_elements_state()

    def extract_members_trigger(self):
        if self.running_extraction:
            self.log_message("Extração já está em andamento.", "warning")
            return
        selected_indices = self.group_listbox.curselection()
        if not selected_indices:
            self.log_message("Nenhum grupo selecionado para extração.", "error")
            messagebox.showerror("Erro", "Selecione pelo menos um grupo para extrair membros.", parent=self.member_window)
            return
        selected_groups = []
        accounts_data = self.main_app.get_operable_accounts()
        group_index = 0
        for acc_data in accounts_data:
            client = acc_data.get('client')
            if client and client.is_connected():
                chats = self.main_app.chats if acc_data.get('phone') == self.main_app.account_var.get() else []
                for chat in chats:
                    if group_index in selected_indices:
                        selected_groups.append((chat, acc_data))
                    group_index += 1
        if not selected_groups:
            self.log_message("Nenhum grupo válido selecionado ou contas não conectadas.", "error")
            messagebox.showerror("Erro", "Nenhum grupo válido ou contas não conectadas.", parent=self.member_window)
            return
        self.running_extraction = True
        self.extract_button.config(state=tk.DISABLED)
        self.log_message(f"Iniciando extração de membros de {len(selected_groups)} grupo(s)...", "info")
        asyncio.run_coroutine_threadsafe(
            self._extract_members_core(selected_groups),
            self.loop
        )

    async def _extract_members_core(self, selected_groups):
        self.extracted_members = []
        member_count = 0
        for chat_detail, acc_data in selected_groups:
            if not self.running_extraction:
                self.log_message("Extração de membros interrompida.", "info")
                break
            client = acc_data.get('client')
            phone = acc_data.get('phone')
            chat_title = chat_detail['title']
            chat_entity = chat_detail['entity']
            self.log_message(f"Conta {phone}: Iniciando extração de '{chat_title}'...", "info")
            try:
                async for member in client.iter_participants(chat_entity, limit=None):
                    if not self.running_extraction:
                        self.log_message(f"Conta {phone}: Extração de '{chat_title}' interrompida.", "info")
                        break
                    user_id = member.id
                    username = f"@{member.username}" if member.username else f"User_{user_id}"
                    self.extracted_members.append((user_id, username, phone))
                    member_count += 1
                    self.log_message(f"Conta {phone}: Membro extraído {username} de '{chat_title}' (ID: {user_id}).", "info")
                    self.member_window.after(0, self.update_member_list_display)
                    await asyncio.sleep(0.1)
                self.log_message(f"Conta {phone}: Extração de '{chat_title}' concluída. Total: {member_count} membros.", "info")
            except FloodWaitError as e_flood:
                self.log_message(f"Conta {phone}: FloodWait ao extrair '{chat_title}': {e_flood.seconds}s. Pausando.", "error")
                await asyncio.sleep(e_flood.seconds + 5)
            except (UserPrivacyRestrictedError, ChatAdminRequiredError) as e_perm:
                self.log_message(f"Conta {phone}: Erro ao extrair '{chat_title}': {type(e_perm).__name__}.", "error")
            except Exception as e:
                self.log_message(f"Conta {phone}: Erro desconhecido ao extrair '{chat_title}': {e}", "error")
        self.member_window.after(0, self._finalize_extraction, member_count)

    def _finalize_extraction(self, member_count):
        self.running_extraction = False
        self.log_message(f"Extração finalizada. Total de membros extraídos: {member_count}.", "info")
        self.update_ui_elements_state()
        if self.member_window and self.member_window.winfo_exists():
            messagebox.showinfo("Extração Concluída", f"Extração finalizada. Total de membros extraídos: {member_count}.", parent=self.member_window)

    def stop_extraction(self):
        if self.running_extraction:
            self.running_extraction = False
            self.log_message("Solicitando parada da extração de membros...", "info")
        else:
            self.log_message("Nenhum processo de extração ativo para parar.", "info")

    def start_mass_sending(self):
        self.log_message("Iniciando verificação para envio em massa...", "debug")
        if self.running_mass_sending:
            self.log_message("Envio em massa já está em andamento.", "warning")
            return
        if not self.extracted_members:
            self.log_message("Nenhuma lista de membros para enviar mensagens.", "error")
            messagebox.showerror("Erro", "Nenhuma lista de membros extraída para enviar mensagens.", parent=self.member_window)
            return
        message_text_content = self.message_text.get("1.0", tk.END).strip()
        if not message_text_content:
            self.log_message("Mensagem vazia para envio em massa.", "error")
            messagebox.showerror("Erro", "A mensagem não pode estar vazia.", parent=self.member_window)
            return
        try:
            interval_s = float(self.interval_entry.get())
            batch_lim = int(self.batch_limit_entry.get())
            pause_m = float(self.pause_duration_entry.get())
            if interval_s < 0 or batch_lim <= 0 or pause_m < 0:
                raise ValueError("Valores de configuração inválidos.")
        except ValueError as ve:
            self.log_message(f"Configuração de envio inválida: {ve}", "error")
            messagebox.showerror("Erro", "Verifique os valores de intervalo, lote e pausa.", parent=self.member_window)
            return
        clients_data_to_use = self.main_app.get_operable_accounts()
        if not clients_data_to_use:
            self.log_message("Nenhuma conta ATIVA configurada para envio.", "error")
            messagebox.showerror("Erro", "Nenhuma conta está marcada como ATIVA no Gerenciador de Status.", parent=self.member_window)
            return
        self.running_mass_sending = True
        self.start_sending_button.config(state=tk.DISABLED)
        self.stop_sending_button.config(state=tk.NORMAL)
        self.log_message(f"Iniciando envio em massa para {len(self.extracted_members)} membros. Contas ATIVAS: {len(clients_data_to_use)}. Intervalo: {interval_s}s, Lote: {batch_lim}, Pausa: {pause_m}min.", "info")
        asyncio.run_coroutine_threadsafe(
            self._sender_coro_mass(message_text_content, list(self.extracted_members), interval_s, batch_lim, pause_s, clients_data_to_use),
            self.loop
        )

    async def _sender_coro_mass(self, message_to_send, members_data_list, interval, batch_limit, pause_duration_sec, clients_data_list_param):
        sent_count_total = 0
        error_count_total = 0
        client_idx = 0
        active_clients_in_run = list(clients_data_list_param)
        if not active_clients_in_run:
            self.log_message("Nenhuma conta cliente conectada e ATIVA fornecida para envio.", "error")
            if self.member_window and self.member_window.winfo_exists():
                self.member_window.after(0, self._finalize_sending, 0, len(members_data_list))
            return
        processed_member_ids_this_run = set()
        member_index_loop = 0
        while member_index_loop < len(members_data_list) and self.running_mass_sending:
            if not self.running_mass_sending:
                self.log_message("Envio em massa interrompido (loop principal).", "info")
                break
            user_id_to_send, username_to_send, _ = members_data_list[member_index_loop]
            if user_id_to_send in processed_member_ids_this_run:
                member_index_loop += 1
                continue
            if not active_clients_in_run:
                self.log_message("Nenhuma conta ativa restante para continuar o envio. Interrompendo.", "warning")
                break
            current_client_data = active_clients_in_run[client_idx % len(active_clients_in_run)]
            client = current_client_data.get('client')
            client_phone = current_client_data.get('phone')
            if not client:
                self.log_message(f"Conta {client_phone}: Cliente não encontrado no loop de envio. Pulando {username_to_send}.", "error")
                error_count_total += 1
                processed_member_ids_this_run.add(user_id_to_send)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                continue
            if not client.is_connected():
                self.log_message(f"Conta {client_phone}: Não está conectada. Tentando reconectar para enviar a {username_to_send}...", "info")
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        self.log_message(f"Conta {client_phone}: Falha na autorização ao reconectar. Tentando próxima conta para {username_to_send}.", "error")
                        client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                        if client_idx == 0 and len(active_clients_in_run) > 1:
                            self.log_message(f"Todas as contas falharam ao conectar/autorizar para {username_to_send}. Pulando membro.", "error")
                            error_count_total += 1
                            processed_member_ids_this_run.add(user_id_to_send)
                            member_index_loop += 1
                        continue
                except Exception as e_conn:
                    self.log_message(f"Conta {client_phone}: Erro ao reconectar - {e_conn}. Tentando próxima conta para {username_to_send}.", "error")
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    if client_idx == 0 and len(active_clients_in_run) > 1:
                        self.log_message(f"Todas as contas falharam ao conectar para {username_to_send}. Pulando membro.", "error")
                        error_count_total += 1
                        processed_member_ids_this_run.add(user_id_to_send)
                        member_index_loop += 1
                    continue
            self.log_message(f"Conta {client_phone}: Tentando enviar mensagem para {username_to_send} (ID: {user_id_to_send})...", 'debug')
            try:
                try:
                    user_entity = await client.get_input_entity(user_id_to_send)
                except ValueError as e_entity:
                    self.log_message(f"Conta {client_phone}: Erro ao resolver entidade para {username_to_send}: {e_entity}. Pulando membro.", "error")
                    error_count_total += 1
                    processed_member_ids_this_run.add(user_id_to_send)
                    member_index_loop += 1
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    continue
                except UsernameNotOccupiedError:
                    self.log_message(f"Conta {client_phone}: Usuário {username_to_send} não existe ou está desativado. Pulando membro.", "warning")
                    error_count_total += 1
                    processed_member_ids_this_run.add(user_id_to_send)
                    member_index_loop += 1
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    continue
                except Exception as e_entity_other:
                    self.log_message(f"Conta {client_phone}: Erro inesperado ao resolver entidade para {username_to_send}: {e_entity_other}. Pulando membro.", "error")
                    error_count_total += 1
                    processed_member_ids_this_run.add(user_id_to_send)
                    member_index_loop += 1
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    continue
                await client.send_message(user_entity, message_to_send)
                self.log_message(f"Conta {client_phone}: Mensagem enviada para {username_to_send} (ID: {user_id_to_send}).", "info")
                sent_count_total += 1
                processed_member_ids_this_run.add(user_id_to_send)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                if sent_count_total > 0 and sent_count_total % batch_limit == 0 and member_index_loop < len(members_data_list):
                    if pause_duration_sec > 0:
                        self.log_message(f"Lote de {sent_count_total} (total) / {batch_limit} (limite) envios atingido. Pausando por {pause_duration_sec/60:.1f} minutos...", "info")
                        pause_until = time.time() + pause_duration_sec
                        while self.running_mass_sending and time.time() < pause_until:
                            await asyncio.sleep(1)
                        if not self.running_mass_sending:
                            self.log_message("Envio interrompido durante pausa do lote.", "info")
                            break
                if self.running_mass_sending and interval > 0 and member_index_loop < len(members_data_list):
                    await asyncio.sleep(interval)
            except FloodWaitError as e_flood:
                self.log_message(f"Conta {client_phone}: FloodWait ao enviar para {username_to_send}: {e_flood.seconds}s. Desativando esta conta e tentando próxima.", "error")
                error_count_total += 1
                account_to_deactivate = self.main_app.get_account_by_phone(client_phone)
                if account_to_deactivate:
                    account_to_deactivate['app_status'] = 'INATIVO'
                    self.main_app.save_accounts()
                active_clients_in_run = [acc for acc in active_clients_in_run if acc.get('phone') != client_phone]
                client_idx = 0
                await asyncio.sleep(e_flood.seconds + 5)
                if not active_clients_in_run:
                    self.log_message("Todas as contas ativas foram desativadas. Interrompendo envio.", "critical")
                    break
            except (UserPrivacyRestrictedError, UserKickedError, UserBannedInChannelError) as e_perm:
                self.log_message(f"Conta {client_phone}: Erro ao enviar para {username_to_send}: {type(e_perm).__name__}. Pulando.", "warning")
                error_count_total += 1
                processed_member_ids_this_run.add(user_id_to_send)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
            except Exception as e_send:
                self.log_message(f"Conta {client_phone}: Erro desconhecido ao enviar para {username_to_send}: {e_send}", "error")
                error_count_total += 1
                processed_member_ids_this_run.add(user_id_to_send)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.after(0, self._finalize_sending, sent_count_total, error_count_total)
        else:
            self._finalize_sending(sent_count_total, error_count_total)

    def _finalize_sending(self, sent_count, error_count):
        self.running_mass_sending = False
        self.log_message(f"Envio em massa finalizado. Enviados com sucesso: {sent_count}, Erros/Falhas: {error_count}.", "info")
        self.update_ui_elements_state()
        if self.member_window and self.member_window.winfo_exists():
            messagebox.showinfo("Envio Concluído", f"Envio em massa finalizado.\nEnviados: {sent_count}\nErros/Falhas: {error_count}", parent=self.member_window)

    def stop_mass_sending(self):
        if self.running_mass_sending:
            self.running_mass_sending = False
            self.log_message("Solicitando parada do envio em massa...", "info")
        else:
            self.log_message("Nenhum processo de envio em massa ativo para parar.", "info")
