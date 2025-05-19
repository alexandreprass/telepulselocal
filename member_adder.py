import tkinter as tk
from tkinter import messagebox, scrolledtext
from ttkbootstrap import ttk, Toplevel, constants as ttk_constants
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError, UserChannelsTooMuchError, 
    UserKickedError, UserBannedInChannelError, ChatAdminRequiredError, 
    UsersTooMuchError, FreshChangePhoneForbiddenError, InviteHashExpiredError,
    RPCError, PeerFloodError 
)
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import InputPeerUser, InputPeerChannel, PeerUser, PeerChat
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
        self.update_members_list_from_manager() 
        
        self.running_addition = False

        self.adder_window = None
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
        logging.debug("MemberAdder __init__ concluída.")

    def setup_ui(self):
        self.adder_window = Toplevel(master=self.main_app.root, title="Adicionar Membros a Grupos")
        self.adder_window.geometry("850x650") 
        self.adder_window.protocol("WM_DELETE_WINDOW", self.on_close)

        main_frame = ttk.Frame(self.adder_window, padding="15")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.adder_window.columnconfigure(0, weight=1)
        self.adder_window.rowconfigure(0, weight=1) 

        top_info_frame = ttk.Frame(main_frame)
        top_info_frame.grid(row=0, column=0, columnspan=2, pady=5, sticky="ew")
        ttk.Label(top_info_frame, text="Operações usarão contas ATIVAS (configuradas no Gerenciador de Status).", bootstyle=ttk_constants.INFO).pack(pady=5)

        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky="nsew", padx=(0,5)) 
        left_frame.rowconfigure(0, weight=1) 
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1) 

        members_to_add_frame = ttk.LabelFrame(left_frame, text="Membros a Serem Adicionados", padding="10", bootstyle=ttk_constants.INFO)
        members_to_add_frame.grid(row=0, column=0, pady=5, sticky="nsew")
        members_to_add_frame.columnconfigure(0, weight=1)
        members_to_add_frame.rowconfigure(0, weight=1)
        self.members_to_add_text = scrolledtext.ScrolledText(members_to_add_frame, width=40, height=15, state='disabled', wrap=tk.WORD)
        self.members_to_add_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.members_to_add_count_label = ttk.Label(members_to_add_frame, text="Total a Adicionar: 0", bootstyle="info-inverse", anchor="e")
        self.members_to_add_count_label.grid(row=1, column=0, padx=5, pady=2, sticky="ew")
        
        # Botões para carregar/extrair membros
        member_load_buttons_frame = ttk.Frame(left_frame)
        member_load_buttons_frame.grid(row=1, column=0, pady=5, sticky="ew")
        member_load_buttons_frame.columnconfigure(0, weight=1)
        member_load_buttons_frame.columnconfigure(1, weight=1)

        ttk.Button(member_load_buttons_frame, text="Recarregar Lista (do Gerenciador)", command=self.update_members_list_from_manager, bootstyle=ttk_constants.SECONDARY)\
            .grid(row=0, column=0, padx=2, pady=2, sticky="ew")
        ttk.Button(member_load_buttons_frame, text="Abrir Gerenciador para Extrair", command=self.open_manager_to_extract, bootstyle=ttk_constants.INFO)\
            .grid(row=0, column=1, padx=2, pady=2, sticky="ew")


        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky="nsew", padx=(5,0)) 
        right_frame.rowconfigure(2, weight=1) 
        main_frame.columnconfigure(1, weight=1)

        target_group_frame = ttk.LabelFrame(right_frame, text="Grupo Alvo (da Conta Selecionada na Janela Principal)", padding="10", bootstyle=ttk_constants.INFO)
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

        add_config_frame = ttk.LabelFrame(right_frame, text="Configurações de Adição", padding="10", bootstyle=ttk_constants.INFO)
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

        log_frame_adder = ttk.LabelFrame(right_frame, text="Log de Adição de Membros", padding="10", bootstyle=ttk_constants.INFO)
        log_frame_adder.grid(row=2, column=0, pady=5, sticky="nsew") 
        log_frame_adder.columnconfigure(0, weight=1)
        log_frame_adder.rowconfigure(0, weight=1)
        self.add_log_text = scrolledtext.ScrolledText(log_frame_adder, width=40, height=10, state='disabled', wrap=tk.WORD) 
        self.add_log_text.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        logging.debug("Janela de adição de membros configurada.")
        self.adder_window.transient(self.main_app.root)
        self.adder_window.grab_set()

    def on_close(self):
        logging.debug("MemberAdder on_close chamado.")
        if self.running_addition: 
            self.stop_member_addition()
        if self.main_app:
            self.main_app.member_adder_instance = None
        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.destroy()

    def reload_main_app_target_chats(self):
        self.log_adder_message("Solicitando recarregamento de grupos alvo da janela principal...", "debug")
        self.main_app.reload_chats_for_selected_account() 
        self.adder_window.after(1000, self._update_target_group_listbox_from_main_app)

    def _update_target_group_listbox_from_main_app(self):
        if self.target_group_listbox and self.target_group_listbox.winfo_exists():
            self.target_group_listbox.delete(0, tk.END)
            if self.main_app.client and self.main_app.client.is_connected(): 
                for chat_detail in self.main_app.chats:
                    self.target_group_listbox.insert(tk.END, chat_detail['title'])
                self.log_adder_message("Lista de grupos alvo atualizada.", "info")
            else:
                self.log_adder_message("Conta selecionada na Janela Principal não conectada. Grupos alvo podem estar desatualizados.", "warning")
        else:
            self.log_adder_message("Widget target_group_listbox não encontrado para atualização.", "warning")

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
        """Abre o MemberManager para que o usuário possa extrair membros."""
        self.log_adder_message("Solicitando abertura do Gerenciador de Membros para extração...", "info")
        self.main_app.open_member_manager() # A MainWindow lida com a criação/foco da instância
        # O usuário precisará extrair lá e depois recarregar aqui.


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
                if self.add_log_text and self.add_log_text.winfo_exists():
                    self.add_log_text.configure(state='normal')
                    self.add_log_text.insert(tk.END, f"{ui_log_msg}\n")
                    self.add_log_text.see(tk.END)
                    self.add_log_text.configure(state='disabled')
            except Exception as e_ui_log:
                logging.error(f"Erro ao logar na UI do MemberAdder: {e_ui_log}", exc_info=True)
        
        if hasattr(self.adder_window, 'after') and self.adder_window.winfo_exists():
            self.adder_window.after(0, _log_to_member_adder_ui)

    def _get_operable_clients_for_adder_action(self):
        operable_accounts_data = self.main_app.get_operable_accounts()
        
        clients_for_action = []
        if not operable_accounts_data:
            self.log_adder_message("Nenhuma conta ATIVA encontrada para a ação de adição.", 'error')
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
                self.log_adder_message(f"Não foi possível obter/inicializar cliente para {acc_data.get('phone')} em MemberAdder. Pulando.", "warning")

        if not clients_for_action:
            self.log_adder_message("Nenhum cliente utilizável encontrado para adição após verificação/inicialização.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                 messagebox.showerror("Nenhum Cliente Utilizável", "Não foi possível preparar nenhum cliente para adição. Verifique status e conexão das contas ATIVAS.", parent=self.adder_window)
        
        return clients_for_action


    def trigger_add_members(self):
        if self.running_addition:
            self.log_adder_message("Adição já está em andamento.", "warning")
            return
        
        self.update_members_list_from_manager() 
        if not self.members_to_add:
            self.log_adder_message("Nenhum membro na lista para adicionar.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Não há membros na lista para adicionar. Extraia ou carregue primeiro no Gerenciador de Membros.", parent=self.adder_window)
            return

        selected_target_indices = self.target_group_listbox.curselection()
        if not selected_target_indices:
            self.log_adder_message("Nenhum grupo alvo selecionado.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Selecione um grupo alvo para adicionar os membros.", parent=self.adder_window)
            return
        
        target_group_index_in_main_chats = selected_target_indices[0]
        try:
            target_chat_detail = self.main_app.chats[target_group_index_in_main_chats] 
            target_chat_title = target_chat_detail['title']
            target_chat_entity_ref = target_chat_detail['entity'] 
        except IndexError:
            self.log_adder_message("Erro ao obter detalhes do grupo alvo. Recarregue a lista de grupos na Janela Principal.", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Grupo alvo inválido. Tente recarregar a lista de grupos da janela principal.", parent=self.adder_window)
            return
        except KeyError:
            self.log_adder_message("Detalhes da entidade do grupo alvo não encontrados. Recarregue os grupos.", "error")
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro", "Informações da entidade do grupo alvo ausentes. Recarregue os grupos.", parent=self.adder_window)
            return


        clients_data_to_use = self._get_operable_clients_for_adder_action()
        if not clients_data_to_use:
            self.log_adder_message("Nenhuma conta ATIVA configurada ou utilizável para adição.", "warning")
            return

        try:
            interval_s = float(self.add_interval_entry.get())
            batch_lim = int(self.add_batch_limit_entry.get())
            pause_m = float(self.add_pause_duration_entry.get())
            if interval_s < 0 or batch_lim <= 0 or pause_m < 0:
                raise ValueError("Valores de configuração de adição inválidos.")
        except ValueError as ve:
            self.log_adder_message(f"Configuração de adição inválida: {ve}", 'error')
            if self.adder_window and self.adder_window.winfo_exists():
                messagebox.showerror("Erro de Configuração", "Verifique os valores de intervalo, lote e pausa.", parent=self.adder_window)
            return
        pause_s = pause_m * 60

        self.running_addition = True
        if self.start_add_button and self.start_add_button.winfo_exists(): 
            self.start_add_button.config(state=tk.DISABLED)
        if self.stop_add_button and self.stop_add_button.winfo_exists(): 
            self.stop_add_button.config(state=tk.NORMAL)
        self.log_adder_message(f"Iniciando adição de {len(self.members_to_add)} membros a '{target_chat_title}'. Contas ATIVAS: {len(clients_data_to_use)}. Intervalo: {interval_s}s, Lote: {batch_lim}, Pausa: {pause_m}min.", 'info')

        asyncio.run_coroutine_threadsafe(
            self._add_members_core(target_chat_entity_ref, target_chat_title, list(self.members_to_add), interval_s, batch_lim, pause_s, clients_data_to_use),
            self.loop
        )

    async def _add_members_core(self, target_chat_entity_ref, target_chat_title_str, members_data_list, interval, batch_limit, pause_duration_sec, clients_data_list_param):
        added_count_total = 0
        error_count_total = 0
        client_idx = 0 

        active_clients_in_run = list(clients_data_list_param) 

        if not active_clients_in_run:
            self.log_adder_message("Nenhuma conta cliente conectada e ATIVA fornecida para _add_members_core.", "error")
            if self.adder_window and self.adder_window.winfo_exists():
                self.adder_window.after(0, self._finalize_member_addition, 0, len(members_data_list))
            return

        processed_member_ids_this_run = set()

        member_index_loop = 0
        while member_index_loop < len(members_data_list) and self.running_addition:
            if not self.running_addition:
                self.log_adder_message("Adição de membros interrompida (loop principal).", "info")
                break
            
            user_id_to_add, username_to_add, _ = members_data_list[member_index_loop]

            if user_id_to_add in processed_member_ids_this_run:
                member_index_loop += 1 
                continue
            
            if not active_clients_in_run: 
                self.log_adder_message("Nenhuma conta ativa restante para continuar a adição. Interrompendo.", "warning")
                break

            current_client_data = active_clients_in_run[client_idx % len(active_clients_in_run)]
            client = current_client_data.get('client')
            client_phone = current_client_data.get('phone')

            if not client:
                self.log_adder_message(f"Conta {client_phone}: Cliente não encontrado no loop de adição. Pulando membro {username_to_add}.", "error")
                error_count_total +=1 
                processed_member_ids_this_run.add(user_id_to_add)
                member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                continue

            if not client.is_connected():
                self.log_adder_message(f"Conta {client_phone}: Não está conectada. Tentando reconectar para adicionar {username_to_add}...", "info")
                try:
                    await client.connect() 
                    if not await client.is_user_authorized():
                        self.log_adder_message(f"Conta {client_phone}: Falha na autorização ao reconectar. Tentando próxima conta para {username_to_add}.", "error")
                        client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                        if client_idx == 0 and len(active_clients_in_run) > 1: 
                             self.log_adder_message(f"Todas as contas falharam ao conectar/autorizar para {username_to_add}. Pulando membro.", "error")
                             error_count_total +=1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                        continue 
                except Exception as e_conn:
                    self.log_adder_message(f"Conta {client_phone}: Erro ao reconectar - {e_conn}. Tentando próxima conta para {username_to_add}.", "error")
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    if client_idx == 0 and len(active_clients_in_run) > 1:
                         self.log_adder_message(f"Todas as contas falharam ao conectar para {username_to_add}. Pulando membro.", "error")
                         error_count_total +=1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                    continue
            
            self.log_adder_message(f"Conta {client_phone}: Tentando adicionar {username_to_add} (ID: {user_id_to_add}) a '{target_chat_title_str}'...", 'debug')
            try:
                # Resolve a entidade do usuário a ser adicionado
                user_to_add_input_entity = await client.get_input_entity(PeerUser(int(user_id_to_add)))
                
                # Resolve a entidade do chat/canal de destino
                target_input_entity = await client.get_input_entity(target_chat_entity_ref)

                if isinstance(target_input_entity, InputPeerChannel):
                    await client(InviteToChannelRequest(channel=target_input_entity, users=[user_to_add_input_entity]))
                elif isinstance(target_input_entity, PeerChat): # Para chats básicos, usamos o ID do chat
                     await client(AddChatUserRequest(chat_id=target_input_entity.chat_id, user_id=user_to_add_input_entity, fwd_limit=10))
                else:
                    self.log_adder_message(f"Conta {client_phone}: Tipo de chat de destino desconhecido ou não suportado para '{target_chat_title_str}' (Tipo: {type(target_input_entity)}). Pulando {username_to_add}.", "error")
                    error_count_total +=1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                    client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
                    continue

                self.log_adder_message(f"Conta {client_phone}: Membro {username_to_add} adicionado a '{target_chat_title_str}'.", 'info')
                added_count_total += 1
                processed_member_ids_this_run.add(user_id_to_add) 
                member_index_loop += 1 
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0

                if added_count_total > 0 and added_count_total % batch_limit == 0 and member_index_loop < len(members_data_list):
                    if pause_duration_sec > 0:
                        self.log_adder_message(f"Lote de {added_count_total} (total) / {batch_limit} (limite) adições atingido. Pausando por {pause_duration_sec/60:.1f} minutos...", 'info')
                        pause_until = time.time() + pause_duration_sec
                        while self.running_addition and time.time() < pause_until: await asyncio.sleep(1)
                        if not self.running_addition: self.log_adder_message("Adição interrompida durante pausa do lote.", "info"); break
                
                if self.running_addition and interval > 0 and member_index_loop < len(members_data_list):
                    await asyncio.sleep(interval)

            except (FloodWaitError, PeerFloodError) as e_flood:
                self.log_adder_message(f"Conta {client_phone}: {type(e_flood).__name__} ({getattr(e_flood, 'seconds', 'N/A')}s) ao adicionar {username_to_add}. Desativando esta conta e tentando próxima.", 'error')
                error_count_total +=1 
                
                account_to_deactivate = self.main_app.get_account_by_phone(client_phone)
                if account_to_deactivate:
                    account_to_deactivate['app_status'] = 'INATIVO'
                    self.main_app.save_accounts()
                
                active_clients_in_run = [acc for acc in active_clients_in_run if acc.get('phone') != client_phone]
                client_idx = 0 
                
                if hasattr(e_flood, 'seconds'): await asyncio.sleep(e_flood.seconds + 5)
                else: await asyncio.sleep(60)
                
                if not active_clients_in_run: self.log_adder_message("Todas as contas ativas foram desativadas. Interrompendo adição.", "critical"); break 
            except (UserPrivacyRestrictedError, UsersTooMuchError, UserChannelsTooMuchError, 
                      FreshChangePhoneForbiddenError, ChatAdminRequiredError, InviteHashExpiredError, 
                      UserKickedError, UserBannedInChannelError, UserAlreadyParticipantError,
                      ValueError, RPCError) as e_specific: # ValueError pode ser de get_input_entity
                 self.log_adder_message(f"Conta {client_phone}: Erro específico ao adicionar {username_to_add} - {type(e_specific).__name__}: {e_specific}. Pulando este membro.", 'warning')
                 error_count_total +=1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                 client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0
            except Exception as e_add: 
                self.log_adder_message(f"Conta {client_phone}: Erro desconhecido ao adicionar {username_to_add} a '{target_chat_title_str}': {e_add}", 'error')
                logging.error(f"Detalhe do erro de adição com {client_phone} para {username_to_add}:", exc_info=True)
                error_count_total += 1; processed_member_ids_this_run.add(user_id_to_add); member_index_loop += 1
                client_idx = (client_idx + 1) % len(active_clients_in_run) if active_clients_in_run else 0

        if self.adder_window and self.adder_window.winfo_exists():
            self.adder_window.after(0, self._finalize_member_addition, added_count_total, error_count_total)
        else:
            self._finalize_member_addition_logic(added_count_total, error_count_total)


    def _finalize_member_addition_logic(self, added_count, error_count):
        self.running_addition = False
        self.log_adder_message(f"Adição de membros finalizada. Adicionados com sucesso: {added_count}, Erros/Falhas em adicionar: {error_count}.", 'info')

    def _finalize_member_addition(self, added_count, error_count):
        self._finalize_member_addition_logic(added_count, error_count)

        if not (self.adder_window and self.adder_window.winfo_exists()):
            return

        if self.start_add_button and self.start_add_button.winfo_exists(): 
            self.start_add_button.config(state=tk.NORMAL)
        if self.stop_add_button and self.stop_add_button.winfo_exists(): 
            self.stop_add_button.config(state=tk.DISABLED)
        
        messagebox.showinfo("Adição Concluída", f"Processo de adição finalizado.\nAdicionados: {added_count}\nErros/Falhas: {error_count}", parent=self.adder_window)

    def stop_member_addition(self):
        if self.running_addition:
            self.running_addition = False
            self.log_adder_message("Solicitando parada da adição de membros...", 'info')
        else:
            self.log_adder_message("Nenhum processo de adição ativo para parar.", "info")

