-- exemplos úteis

-- 1) Reimprimir lista/etiquetas apenas do dia atual
SELECT v.nf_numero, v.pedido_id, v.numero_pedido, v.cliente_nome, v.marketplace, v.received_at, i.descricao
FROM public.vw_nf_pedido_expandida v
JOIN public.omie_pedido_itens i ON i.pedido_id = v.pedido_id
WHERE DATE(v.received_at) = CURRENT_DATE
ORDER BY v.received_at;

-- 2) Ver o que já foi impresso recentemente
SELECT * FROM public.etiqueta_log ORDER BY printed_at DESC LIMIT 30;
