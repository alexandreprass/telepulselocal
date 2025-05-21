import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants

from pyrogram.errors import ( # Adicionado Pyrogram errors
    FloodWait, UserPrivacyRestricted, UserKicked, UserBannedInChannel, # UserNotMutual REMOVIDO daqui
    ChatAdminRequired, UsernameNotOccupied, UserChannelsTooMuch, UserIdInvalid,
    PeerFlood, ChannelInvalid, ChannelPrivate, MsgIdInvalid, InviteRequestSent,
    UserAlreadyParticipant, ChatWriteForbidden, UserDeactivatedBan, BadRequest
)
from pyrogram.enums import ChatMembersFilter, ChatType # Adicionado Pyrogram enums
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
        self.running_private_sending = False

        self.member_window = None
        self.group_listbox = None
        self.members_text = None
        self.members_count_label = None
        self.private_message_text = None
        self.interval_entry = None
        self.batch_limit_entry = None
        self.pause_duration_entry = None
        self.private_log_text = None
        self.extract_button = None
        self.start_private_button = None
        self.stop_private_button = None

        self.setup_ui()
        logging.debug("MemberManager __init__ concluída.")

    def setup_ui(self):
        self.member_window = Toplevel(master=self.main_app.root, title="Gerenciar Membros de Grupos (Pyrogram)")
        self.member_window.geometry("850x650")
        self.member_window.protocol("WM_DELETE_WINDOW", self.on_close)

        main_frame = ttk.Frame(self.member_window, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.member_window.columnconfigure(0, weight=1)
        self.member_window.rowconfigure(0, weight=1)

        top_controls_frame = ttk.Frame(main_frame)
        top_controls_frame.grid(row=0, column=0, columnspan=2, pady=5, sticky="ew")
        ttk.Label(top_controls_frame, text="Extração usará a conta selecionada na Janela Principal.\nEnvio Privado usará contas ATIVAS do Gerenciador de Status.", bootstyle=ttk_constants.INFO).pack(pady=2)

        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky="nsew", padx=(0,5))
        left_frame.rowconfigure(1, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)

        group_frame = ttk.LabelFrame(left_frame, text="Grupo Fonte (da Conta Selecionada na Janela Principal)", padding="10", bootstyle=ttk_constants.INFO)
        group_frame.grid(row=0, column=0, pady=5, sticky="ew")
        group_frame.columnconfigure(0, weight=1)
        self.group_listbox = tk.Listbox(group_frame, selectmode=tk.SINGLE, width=35, height=5, exportselection=False)
        self.group_listbox.grid(row=0, column=0, padx=(5,0), pady=5, sticky="nsew")
        scrollbar = ttk.Scrollbar(group_frame, orient=tk.VERTICAL, command=self.group_listbox.yview, bootstyle="round-info")
        scrollbar.grid(row=0, column=1, sticky="ns", pady=5, padx=(0,5))
        self.group_listbox.config(yscrollcommand=scrollbar.set)

        self._update_group_listbox_from_main_app()

        members_frame = ttk.LabelFrame(left_frame, text="Membros Extraídos", padding="10", bootstyle=ttk_constants.INFO)
        members_frame.grid(row=1, column=0, pady=5, sticky="nsew")
        members_frame.columnconfigure(0, weight=1)
        members_frame.rowconfigure(0, weight=1)
        self.members_text = scrolledtext.ScrolledText(members_frame, width=35, height=10, state='disabled', wrap=tk.WORD)
        self.members_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.members_count_label = ttk.Label(members_frame, text="Total Extraído: 0", bootstyle="info-inverse", anchor="e")
        self.members_count_label.grid(row=1, column=0, padx=5, pady=2, sticky="ew")

        button_frame_left = ttk.Frame(left_frame)
        button_frame_left.grid(row=2, column=0, pady=10, sticky="ew")
        button_frame_left.columnconfigure(0, weight=1)
        ttk.Button(button_frame_left, text="Recarregar Grupos Fonte (da Janela Principal)", command=self.reload_main_app_chats, bootstyle=ttk_constants.SECONDARY)\
            .grid(row=0, column=0, columnspan=2, padx=2, pady=2, sticky="ew")

        extract_buttons_frame = ttk.Frame(button_frame_left)
        extract_buttons_frame.grid(row=1, column=0, columnspan=2, sticky="ew")
        extract_buttons_frame.columnconfigure(0, weight=1)
        self.extract_button = ttk.Button(extract_buttons_frame, text="Extrair Membros (Conta da Janela Principal)", command=self.trigger_extract_members, bootstyle=ttk_constants.PRIMARY)
        self.extract_button.grid(row=0, column=0, padx=2, pady=2, sticky="ew")

        ttk.Button(button_frame_left, text="Limpar Lista Extraída", command=self.clear_extracted_contacts, bootstyle=ttk_constants.WARNING)\
            .grid(row=2, column=0, columnspan=2, padx=2, pady=2, sticky="ew")

        file_buttons_frame = ttk.Frame(button_frame_left)
        file_buttons_frame.grid(row=3, column=0, columnspan=2, sticky="ew")
        file_buttons_frame.columnconfigure(0, weight=1)
        file_buttons_frame.columnconfigure(1, weight=1)
        ttk.Button(file_buttons_frame, text="Salvar Membros", command=self.save_members_to_file, bootstyle=ttk_constants.SUCCESS)\
            .grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        ttk.Button(file_buttons_frame, text="Carregar Membros", command=self.load_members_from_file, bootstyle=ttk_constants.SUCCESS)\
            .grid(row=0, column=1, padx=2, pady=2, sticky="ew")

        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky="nsew", padx=(5,0))
        right_frame.rowconfigure(1, weight=1)
        main_frame.columnconfigure(1, weight=1)

        private_msg_frame = ttk.LabelFrame(right_frame, text="Enviar Mensagem Privada (Usar Contas ATIVAS)", padding="10", bootstyle=ttk_constants.INFO)
        private_msg_frame.grid(row=0, column=0, pady=5, sticky="ew")
        private_msg_frame.columnconfigure(0, weight=1)
        self.private_message_text = scrolledtext.ScrolledText(private_msg_frame, width=40, height=6, wrap=tk.WORD)
        self.private_message_text.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

        config_frame_right = ttk.Frame(private_msg_frame)
        config_frame_right.grid(row=1, column=0, pady=5, sticky="ew")
        ttk.Label(config_frame_right, text="Intervalo (s):").grid(row=0, column=0, padx=(5,2), pady=2, sticky="w")
        self.interval_entry = ttk.Entry(config_frame_right, width=5)
        self.interval_entry.insert(0, "10")
        self.interval_entry.grid(row=0, column=1, padx=(0,5), pady=2, sticky="w")

        ttk.Label(config_frame_right, text="Lote:").grid(row=0, column=2, padx=(5,2), pady=2, sticky="w")
        self.batch_limit_entry = ttk.Entry(config_frame_right, width=5)
        self.batch_limit_entry.insert(0, "10")
        self.batch_limit_entry.grid(row=0, column=3, padx=(0,5), pady=2, sticky="w")

        ttk.Label(config_frame_right, text="Pausa (min):").grid(row=0, column=4, padx=(5,2), pady=2, sticky="w")
        self.pause_duration_entry = ttk.Entry(config_frame_right, width=5)
        self.pause_duration_entry.insert(0, "5")
        self.pause_duration_entry.grid(row=0, column=5, padx=(0,5), pady=2, sticky="w")

        send_buttons_right_frame = ttk.Frame(private_msg_frame)
        send_buttons_right_frame.grid(row=2, column=0, pady=5, sticky="ew")
        send_buttons_right_frame.columnconfigure(0,weight=1)
        send_buttons_right_frame.columnconfigure(1,weight=1)
        self.start_private_button = ttk.Button(send_buttons_right_frame, text="Enviar Privadas", command=self.trigger_start_private_sending, bootstyle=ttk_constants.PRIMARY)
        self.start_private_button.grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        self.stop_private_button = ttk.Button(send_buttons_right_frame, text="Parar Envio", command=self.stop_private_sending, bootstyle=ttk_constants.DANGER, state=tk.DISABLED)
        self.stop_private_button.grid(row=0, column=1, padx=2, pady=2, sticky="ew")

        log_frame_right = ttk.LabelFrame(right_frame, text="Log de Ações (Gerenciador de Membros)", padding="10", bootstyle=ttk_constants.INFO)
        log_frame_right.grid(row=1, column=0, pady=5, sticky="nsew")
        log_frame_right.columnconfigure(0, weight=1)
        log_frame_right.rowconfigure(0, weight=1)
        self.private_log_text = scrolledtext.ScrolledText(log_frame_right, width=40, height=10, state='disabled', wrap=tk.WORD)
        self.private_log_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")

        logging.debug("Janela de gerenciamento de membros configurada.")
        self.member_window.transient(self.main_app.root)
        self.member_window.grab_set()

    def on_close(self):
        logging.debug("MemberManager on_close chamado.")
        if self.running_extraction:
            self.running_extraction = False
            self.private_log_message("Extração interrompida pelo fechamento da janela.", "info")
            if self.extract_button and self.extract_button.winfo_exists():
                self.extract_button.config(state=tk.NORMAL)
        if self.running_private_sending:
            self.stop_private_sending()

        if self.main_app:
            self.main_app.member_manager_instance = None
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.destroy()

    def reload_main_app_chats(self):
        self.private_log_message("Solicitando recarregamento de grupos da janela principal...", "debug")
        self.main_app.reload_chats_for_selected_account()
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.after(1500, self._update_group_listbox_from_main_app)

    def _update_group_listbox_from_main_app(self):
        if self.group_listbox and self.group_listbox.winfo_exists():
            self.group_listbox.delete(0, tk.END)
            if self.main_app.client and self.main_app.client.is_connected:
                for chat_detail in self.main_app.chats:
                    self.group_listbox.insert(tk.END, chat_detail['title'])
                self.private_log_message("Lista de grupos fonte atualizada (Pyrogram).", "info")
            else:
                self.private_log_message("Conta selecionada na Janela Principal não conectada. Grupos fonte podem estar desatualizados (Pyrogram).", "warning")
        else:
            self.private_log_message("Widget group_listbox não encontrado para atualização (Pyrogram).", "warning")

    def _get_operable_clients_for_action(self):
        operable_accounts_data = self.main_app.get_operable_accounts()

        clients_for_action = []
        if not operable_accounts_data:
            self.private_log_message("Nenhuma conta ATIVA encontrada para a ação (Pyrogram).", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Nenhuma Conta Ativa", "Nenhuma conta está marcada como ATIVA no Gerenciador de Status.", parent=self.member_window)
            return []

        for acc_data in operable_accounts_data:
            client_obj = acc_data.get('client')
            if not client_obj:
                self.main_app.initialize_client_for_account_data(acc_data)
                client_obj = acc_data.get('client')

            if client_obj:
                clients_for_action.append(acc_data)
            else:
                self.private_log_message(f"Não foi possível obter/inicializar cliente Pyrogram para {acc_data.get('phone')}. Pulando esta conta.", "warning")

        if not clients_for_action:
            self.private_log_message("Nenhum cliente Pyrogram utilizável encontrado após verificação/inicialização.", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Nenhum Cliente Utilizável", "Não foi possível preparar nenhum cliente Pyrogram para a ação. Verifique o status e a conexão das contas ATIVAS.", parent=self.member_window)
        return clients_for_action

    def private_log_message(self, message, level='info'):
        self.main_app.log_message(f"[MemberManager] {message}", level)
        timestamp_str = datetime.datetime.now().strftime("%H:%M:%S")
        ui_log_msg = f"[{timestamp_str}] {message}"

        is_manager_log = True
        if "[MemberAdder]" in message or "[AccountStatusManager]" in message:
            is_manager_log = False

        if is_manager_log:
            if "Membro(s) extraído(s)" in message or "Membros salvos" in message or "Membros carregados" in message:
                pass
            elif "Mensagem enviada para @" in message:
                parts = message.split("para ")
                if len(parts) > 1:
                    target_info = parts[1].strip()
                    ui_log_msg = f"[{timestamp_str}] {target_info} - SUCESSO (PV) ✅"
            elif "Erro ao enviar para @" in message:
                parts = message.split("para ")
                if len(parts) > 1:
                    target_info_parts = parts[1].split(":")
                    target_info = target_info_parts[0].strip()
                    error_detail = target_info_parts[1].strip() if len(target_info_parts) > 1 else "Erro"
                    ui_log_msg = f"[{timestamp_str}] {target_info} - ERRO (PV) ❌ ({error_detail})"

        def _log_to_member_manager_ui():
            try:
                if self.private_log_text and self.private_log_text.winfo_exists():
                    self.private_log_text.configure(state='normal')
                    self.private_log_text.insert(tk.END, f"{ui_log_msg}\n")
                    self.private_log_text.see(tk.END)
                    self.private_log_text.configure(state='disabled')
            except Exception as e_ui_log:
                logging.error(f"Erro ao logar na UI do MemberManager: {e_ui_log}", exc_info=True)

        if hasattr(self.member_window, 'after') and self.member_window.winfo_exists():
            self.member_window.after(0, _log_to_member_manager_ui)

    def clear_extracted_contacts(self):
        self.extracted_members = []
        if self.members_text and self.members_text.winfo_exists():
            self.members_text.configure(state='normal')
            self.members_text.delete("1.0", tk.END)
            self.members_text.configure(state='disabled')
        if self.members_count_label and self.members_count_label.winfo_exists():
            self.members_count_label.config(text="Total Extraído: 0")
        self.private_log_message("Lista de contatos extraídos limpa.", 'info')
        if self.member_window and self.member_window.winfo_exists():
            messagebox.showinfo("Sucesso", "Contatos extraídos foram limpos.", parent=self.member_window)

    def save_members_to_file(self):
        if not self.extracted_members:
            self.private_log_message("Nenhum membro extraído para salvar.", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", "Extraia membros antes de salvar.", parent=self.member_window)
            return

        members_data_to_save = []
        for user_id, username_str, source_phone_info in self.extracted_members:
            members_data_to_save.append({
                "user_id": user_id,
                "username": username_str,
                "source_phone": source_phone_info
            })

        try:
            filename = filedialog.asksaveasfilename(
                defaultextension=".json",
                filetypes=[("JSON files", "*.json")],
                initialfile="extracted_members_pyrogram.json",
                parent=self.member_window,
                title="Salvar Lista de Membros"
            )
            if not filename:
                self.private_log_message("Salvamento de membros cancelado.", 'info')
                return
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(members_data_to_save, f, ensure_ascii=False, indent=2)
            self.private_log_message(f"Membros ({len(members_data_to_save)}) salvos em {filename}.", 'info')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showinfo("Sucesso", f"{len(members_data_to_save)} membros salvos em {filename}.", parent=self.member_window)
        except Exception as e:
            self.private_log_message(f"Erro ao salvar membros: {e}", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", f"Falha ao salvar membros: {e}", parent=self.member_window)

    def load_members_from_file(self):
        try:
            filename = filedialog.askopenfilename(
                filetypes=[("JSON files", "*.json")],
                parent=self.member_window,
                title="Carregar Lista de Membros"
            )
            if not filename:
                self.private_log_message("Carregamento de membros cancelado.", 'info')
                return
            with open(filename, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)

            if not isinstance(loaded_data, list):
                raise ValueError("Formato de arquivo inválido. Esperada uma lista de membros.")

            self.clear_extracted_contacts()
            count = 0
            for member_info in loaded_data:
                user_id = member_info.get("user_id")
                username = member_info.get("username")
                source_phone = member_info.get("source_phone", "Desconhecida")
                if user_id and username:
                    if not any(m[0] == user_id for m in self.extracted_members):
                        self.extracted_members.append((user_id, username, source_phone))
                        if self.members_text and self.members_text.winfo_exists():
                             self.members_text.configure(state='normal')
                             self.members_text.insert(tk.END, f"{username} (ID: {user_id}, Fonte: {source_phone})\n")
                             self.members_text.configure(state='disabled')
                        count += 1
                    else:
                        self.private_log_message(f"Membro duplicado (ID: {user_id}) não carregado do arquivo.", "debug")

            if self.members_count_label and self.members_count_label.winfo_exists():
                self.members_count_label.config(text=f"Total Carregado: {count}")
            self.private_log_message(f"{count} membros únicos carregados de {filename}.", 'info')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showinfo("Sucesso", f"{count} membros únicos carregados de {filename}.", parent=self.member_window)
        except Exception as e:
            self.private_log_message(f"Erro ao carregar membros: {e}", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", f"Falha ao carregar membros: {e}", parent=self.member_window)

    def trigger_extract_members(self):
        if self.running_extraction:
            self.private_log_message("Extração já está em andamento (Pyrogram).", "warning")
            return

        selected_indices_source_group = self.group_listbox.curselection()
        if not selected_indices_source_group:
            self.private_log_message("Nenhum grupo fonte selecionado para extração (Pyrogram).", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", "Selecione um grupo fonte da lista.", parent=self.member_window)
            return

        source_group_index_in_main_chats = selected_indices_source_group[0]
        try:
            source_chat_detail = self.main_app.chats[source_group_index_in_main_chats]
            source_chat_identifier = source_chat_detail['id']
            source_chat_title = source_chat_detail['title']
        except (IndexError, KeyError) as e:
            self.private_log_message(f"Erro ao obter detalhes do grupo fonte (Pyrogram) ({e}). Recarregue a lista de grupos na Janela Principal.", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", "Grupo fonte inválido ou detalhes ausentes. Tente recarregar a lista de grupos da janela principal.", parent=self.member_window)
            return

        client_for_extraction = self.main_app.client
        phone_of_extraction_client = self.main_app.account_var.get()

        if not (client_for_extraction and client_for_extraction.is_connected):
            self.private_log_message(f"A conta selecionada na Janela Principal ({phone_of_extraction_client}) não está conectada (Pyrogram). Conecte-a primeiro.", "error")
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Conta Desconectada", f"A conta {phone_of_extraction_client} (selecionada na Janela Principal) precisa estar conectada para extrair membros.", parent=self.member_window)
            return

        self.running_extraction = True
        self.private_log_message(f"Iniciando extração (Pyrogram) de '{source_chat_title}' (ID: {source_chat_identifier}) usando a conta {phone_of_extraction_client}...", 'info')
        if self.extract_button and self.extract_button.winfo_exists():
            self.extract_button.config(state=tk.DISABLED)
        self.clear_extracted_contacts()
        client_data_for_extraction = {
            'client': client_for_extraction,
            'phone': phone_of_extraction_client
        }
        asyncio.run_coroutine_threadsafe(
            self._extract_members_core(source_chat_identifier, source_chat_title, client_data_for_extraction),
            self.loop
        )

    async def _extract_members_core(self, source_chat_id, source_chat_title, client_data_dict):
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.after(0, lambda: (
                self.members_text.configure(state='normal') if self.members_text and self.members_text.winfo_exists() else None,
                self.members_text.delete("1.0", tk.END) if self.members_text and self.members_text.winfo_exists() else None,
                self.members_text.configure(state='disabled') if self.members_text and self.members_text.winfo_exists() else None,
                self.members_count_label.config(text="Extraindo... 0") if self.members_count_label and self.members_count_label.winfo_exists() else None
            ))

        globally_extracted_user_ids = set()
        total_newly_extracted_this_run = 0
        client = client_data_dict.get('client')
        phone_of_client = client_data_dict.get('phone')

        if not client or not client.is_connected:
            self.private_log_message(f"Conta {phone_of_client}: Cliente Pyrogram não disponível ou desconectado para extração.", "error")
            self.running_extraction = False
            if self.extract_button and self.extract_button.winfo_exists():
                self.extract_button.config(state=tk.NORMAL)
            return

        self.private_log_message(f"Conta {phone_of_client}: Extraindo membros de '{source_chat_title}' (ID: {source_chat_id}) (Pyrogram)...", 'info')
        current_account_extracted_count = 0
        try:
            async for member in client.get_chat_members(source_chat_id):
                if not self.running_extraction: break
                user = member.user
                if user.is_bot or not user.username:
                    continue
                if user.id not in globally_extracted_user_ids:
                    globally_extracted_user_ids.add(user.id)
                    member_tuple = (user.id, f"@{user.username}", phone_of_client)
                    self.extracted_members.append(member_tuple)
                    total_newly_extracted_this_run += 1
                    current_account_extracted_count += 1
                    if total_newly_extracted_this_run % 20 == 0:
                        if self.member_window and self.member_window.winfo_exists():
                            self.member_window.after(0, lambda u=f"@{user.username}", i=user.id, s=phone_of_client, c=total_newly_extracted_this_run: (
                                self.members_text.configure(state='normal') if self.members_text and self.members_text.winfo_exists() else None,
                                self.members_text.insert(tk.END, f"{u} (ID: {i}, Fonte: {s})\n") if self.members_text and self.members_text.winfo_exists() else None,
                                self.members_text.see(tk.END) if self.members_text and self.members_text.winfo_exists() else None,
                                self.members_text.configure(state='disabled') if self.members_text and self.members_text.winfo_exists() else None,
                                self.members_count_label.config(text=f"Total Extraído: {c}") if self.members_count_label and self.members_count_label.winfo_exists() else None
                            ))
            if not self.running_extraction:
                 self.private_log_message(f"Extração de '{source_chat_title}' interrompida pela conta {phone_of_client} (Pyrogram).", "info")
        except FloodWait as e_flood:
            self.private_log_message(f"Conta {phone_of_client}: FloodWait ({e_flood.value}s) ao extrair de '{source_chat_title}' (Pyrogram). Pausando.", 'error')
            await asyncio.sleep(e_flood.value + 10)
        except (ChatAdminRequired, ChannelInvalid, ChannelPrivate, BadRequest) as e_perm_rpc:
             self.private_log_message(f"Conta {phone_of_client}: Erro de permissão/entidade ao extrair de '{source_chat_title}' (Pyrogram) ({type(e_perm_rpc).__name__}: {e_perm_rpc}).", 'warning')
        except Exception as e:
            self.private_log_message(f"Conta {phone_of_client}: Erro inesperado ao extrair de '{source_chat_title}' (Pyrogram) - {e}", 'error')
            logging.error(f"Detalhe do erro de extração (Pyrogram) com {phone_of_client} para '{source_chat_title}':", exc_info=True)

        if self.running_extraction:
            self.private_log_message(f"Conta {phone_of_client}: {current_account_extracted_count} novos membros únicos adicionados de '{source_chat_title}' (Pyrogram).", 'info')
        self.running_extraction = False

        def update_ui_after_all_extractions_final():
            if not (self.member_window and self.member_window.winfo_exists()): return
            try:
                if self.members_text and self.members_text.winfo_exists():
                    self.members_text.configure(state='normal')
                    self.members_text.delete("1.0", tk.END)
                    for uid, uname, sphone in self.extracted_members:
                        self.members_text.insert(tk.END, f"{uname} (ID: {uid}, Fonte: {sphone})\n")
                    self.members_text.see(tk.END)
                    self.members_text.configure(state='disabled')
                if self.members_count_label and self.members_count_label.winfo_exists():
                    self.members_count_label.config(text=f"Total Extraído: {len(self.extracted_members)}")
                self.private_log_message(f"Extração (Pyrogram) concluída. Total de {len(self.extracted_members)} usuários únicos extraídos de '{source_chat_title}'.", 'info')
                if not self.extracted_members:
                     self.private_log_message(f"Nenhum membro (com username, não-bot) encontrado em '{source_chat_title}' com a conta {phone_of_client} (Pyrogram).", 'warning')
                if self.extract_button and self.extract_button.winfo_exists():
                    self.extract_button.config(state=tk.NORMAL)
            except Exception as e_ui:
                logging.error(f"Erro ao atualizar UI após extração total (MemberManager - Pyrogram): {e_ui}", exc_info=True)
                if self.extract_button and self.extract_button.winfo_exists():
                    self.extract_button.config(state=tk.NORMAL)
        if self.member_window and self.member_window.winfo_exists():
            self.member_window.after(0, update_ui_after_all_extractions_final)

    def trigger_start_private_sending(self):
        if self.running_private_sending:
            self.private_log_message("Envio privado já está em andamento (Pyrogram).", "warning")
            return
        if not self.extracted_members:
            self.private_log_message("Nenhum usuário extraído para envio de mensagem privada (Pyrogram).", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", "Extraia ou carregue usuários antes de enviar mensagens.", parent=self.member_window)
            return

        message_content = self.private_message_text.get("1.0", tk.END).strip()
        if not message_content:
            self.private_log_message("Mensagem privada vazia (Pyrogram).", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro", "A mensagem privada não pode estar vazia.", parent=self.member_window)
            return

        try:
            interval_s = float(self.interval_entry.get())
            batch_lim = int(self.batch_limit_entry.get())
            pause_m = float(self.pause_duration_entry.get())
            if interval_s < 0 or batch_lim <= 0 or pause_m < 0:
                raise ValueError("Valores de configuração de envio inválidos.")
        except ValueError as ve:
            self.private_log_message(f"Configuração de envio inválida (Pyrogram): {ve}", 'error')
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Erro de Configuração", "Verifique os valores de intervalo, lote e pausa.", parent=self.member_window)
            return
        pause_s = pause_m * 60
        initial_clients_data_to_use = self._get_operable_clients_for_action()
        if not initial_clients_data_to_use:
            self.private_log_message("Nenhuma conta ATIVA configurada ou utilizável para envio privado (Pyrogram).", "warning")
            return

        connected_clients_for_sending = []
        for acc_data in initial_clients_data_to_use:
            client = acc_data.get('client')
            if client and client.is_connected:
                connected_clients_for_sending.append(acc_data)
            else:
                self.private_log_message(f"Conta {acc_data.get('phone')} está ATIVA mas não conectada (Pyrogram). Não será usada para este envio privado.", "warning")

        if not connected_clients_for_sending:
            self.private_log_message("Nenhuma conta ATIVA e CONECTADA encontrada para iniciar o envio privado (Pyrogram).", "error")
            if self.member_window and self.member_window.winfo_exists():
                messagebox.showerror("Nenhuma Conta Pronta", "Nenhuma conta ATIVA está conectada. Conecte as contas no Gerenciador de Status.", parent=self.member_window)
            return

        self.running_private_sending = True
        if self.start_private_button and self.start_private_button.winfo_exists():
            self.start_private_button.config(state=tk.DISABLED)
        if self.stop_private_button and self.stop_private_button.winfo_exists():
            self.stop_private_button.config(state=tk.NORMAL)
        if self.extract_button and self.extract_button.winfo_exists():
            self.extract_button.config(state=tk.DISABLED)
        self.private_log_message(f"Iniciando envio privado (Pyrogram) para {len(self.extracted_members)} usuários. Contas conectadas e ATIVAS: {len(connected_clients_for_sending)}. Intervalo: {interval_s}s, Lote: {batch_lim}, Pausa: {pause_m}min.", 'info')
        asyncio.run_coroutine_threadsafe(
            self._private_sender_core(message_content, list(self.extracted_members), interval_s, batch_lim, pause_s, connected_clients_for_sending),
            self.loop
        )

    async def _private_sender_core(self, message_to_send, members_to_process_list, interval, batch_limit, pause_duration_sec, clients_data_for_this_run):
        sent_total_count = 0
        error_total_count = 0
        client_idx = 0
        active_clients_in_run = list(clients_data_for_this_run)

        if not active_clients_in_run:
            self.private_log_message("Nenhuma conta cliente Pyrogram conectada e ATIVA fornecida para _private_sender_core.", "error")
            if self.member_window and self.member_window.winfo_exists():
                self.member_window.after(0, self._finalize_private_sending, 0, len(members_to_process_list))
            return

        for i, (user_id, user_info_str, _) in enumerate(members_to_process_list):
            if not self.running_private_sending:
                self.private_log_message("Envio privado interrompido pelo usuário (Pyrogram).", "info")
                break
            if not active_clients_in_run:
                self.private_log_message("Nenhuma conta ativa restante para continuar o envio (Pyrogram). Interrompendo.", "warning")
                break

            current_account_data = active_clients_in_run[client_idx % len(active_clients_in_run)]
            client = current_account_data.get('client')
            client_phone = current_account_data.get('phone')

            if not client or not client.is_connected:
                self.private_log_message(f"Conta {client_phone}: Cliente Pyrogram desconectado ou não encontrado. Removendo da sessão de envio.", "error")
                error_total_count += 1
                active_clients_in_run = [acc for acc in active_clients_in_run if acc.get('phone') != client_phone]
                if not active_clients_in_run: break
                client_idx = 0
                continue

            self.private_log_message(f"Conta {client_phone}: Tentando enviar para {user_info_str} (ID/User: {user_id}) (Pyrogram)...", 'debug')
            try:
                await client.send_message(chat_id=user_id, text=message_to_send)
                self.private_log_message(f"Conta {client_phone}: Mensagem enviada para {user_info_str} (Pyrogram).", 'info')
                sent_total_count += 1
            except FloodWait as e_flood:
                self.private_log_message(f"Conta {client_phone}: {type(e_flood).__name__} ({e_flood.value}s) ao enviar para {user_info_str}. Desativando e removendo da sessão.", 'error') # .value
                error_total_count += 1
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
                    self.private_log_message("Todas as contas ativas foram desativadas (Pyrogram). Interrompendo envio.", "critical")
                    break
                continue
            # UserNotMutual REMOVIDO DAQUI
            except (UserPrivacyRestricted, UsernameNotOccupied, UserIdInvalid, UserDeactivatedBan, BadRequest) as e_user_issue:
                self.private_log_message(f"Conta {client_phone}: Não foi possível enviar para {user_info_str} (Pyrogram) ({type(e_user_issue).__name__}: {e_user_issue}). Pulando.", 'warning')
                error_total_count += 1
            except Exception as e_send:
                self.private_log_message(f"Conta {client_phone}: Erro desconhecido ao enviar para {user_info_str} (Pyrogram): {e_send}", 'error')
                logging.error(f"Detalhe do erro de envio privado (Pyrogram) com {client_phone} para {user_info_str}:", exc_info=True)
                error_total_count += 1

            if active_clients_in_run:
                 client_idx = (client_idx + 1) % len(active_clients_in_run)

            if sent_total_count > 0 and sent_total_count % batch_limit == 0 and (i + 1) < len(members_to_process_list):
                if pause_duration_sec > 0 :
                    self.private_log_message(f"Lote de {sent_total_count} (total) / {batch_limit} (limite) envios atingido (Pyrogram). Pausando por {pause_duration_sec/60:.1f} minutos...", 'info')
                    pause_until = time.time() + pause_duration_sec
                    while self.running_private_sending and time.time() < pause_until:
                        await asyncio.sleep(1)
                    if not self.running_private_sending:
                         self.private_log_message("Envio privado interrompido durante a pausa do lote (Pyrogram).", "info")
                         break
            if self.running_private_sending and interval > 0 and (i + 1) < len(members_to_process_list):
                await asyncio.sleep(interval)

        if self.member_window and self.member_window.winfo_exists():
            self.member_window.after(0, self._finalize_private_sending, sent_total_count, error_total_count)
        else:
            self._finalize_private_sending_logic(sent_total_count, error_total_count)

    def _finalize_private_sending_logic(self, sent_count, error_count):
        self.running_private_sending = False
        total_processed = sent_count + error_count
        self.private_log_message(f"Envio privado finalizado (Pyrogram). Total processado: {total_processed}. Enviados: {sent_count}, Erros: {error_count}.", 'info')

    def _finalize_private_sending(self, sent_count, error_count):
        self._finalize_private_sending_logic(sent_count, error_count)
        if not (self.member_window and self.member_window.winfo_exists()):
            return
        if self.start_private_button and self.start_private_button.winfo_exists():
            self.start_private_button.config(state=tk.NORMAL)
        if self.stop_private_button and self.stop_private_button.winfo_exists():
            self.stop_private_button.config(state=tk.DISABLED)
        if self.extract_button and self.extract_button.winfo_exists():
            self.extract_button.config(state=tk.NORMAL)
        if self.member_window and self.member_window.winfo_exists():
            messagebox.showinfo("Envio Privado Concluído", f"Processo finalizado (Pyrogram).\nEnviados: {sent_count}\nErros: {error_count}", parent=self.member_window)

    def stop_private_sending(self):
        if self.running_private_sending:
            self.running_private_sending = False
            self.private_log_message("Solicitando parada do envio de mensagens privadas (Pyrogram)...", 'info')
        else:
            self.private_log_message("Nenhum processo de envio privado ativo para parar (Pyrogram).", "info")
