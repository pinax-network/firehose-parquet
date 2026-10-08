# HyperCore fixture blocks

Each `<block>.pb` file is the unchanged `google.protobuf.Any.value` payload of a
`sf.firehose.v2.Stream/Blocks` response from
`hypercore.firehose.pinax.network:443`, requested with `final_blocks_only=true`
and decoded as `type.googleapis.com/pinax.hypercore.v1.Block` (`proto/pinax/hypercore/v1/`,
firehose-hypercore `a4ade4d`). They were captured on **2026-10-06**; nothing was
re-encoded. No response cursor, credential or request header is retained.

`846903317.pb.zst` is the 12,362,099-byte funding block of 2026-01-01T00:00:00Z,
compressed with `zstd -19` (3,249,446 bytes, SHA-256 of the compressed file
`4b21394ad565965873f4162f5e3a93fa60d35b36801ab5b9917daebae755b145`). The checksums below are those of the decompressed payloads; the tests
decompress the file and verify its checksum, so any conforming zstd encoder can
recompress it.

The 36 blocks were chosen by a greedy set cover over about 2,400 candidate blocks
from about 9.3 million sampled ones, preferring small blocks. Together they cover
every event body (8), every ledger delta (22), 19 of the 22 fill directions (not
`LIQUIDATED_CROSS_SHORT`, `BACKSTOP_BORROW_LIQUIDATION` or
`PARTIAL_BORROW_LIQUIDATION`, never seen on the endpoint), both sides, both leverage
types and both liquidation methods. `1009855075` (a delisted-perp `SETTLEMENT`
with the zero address) was added by hand. `1075987296` (`hip3_liquidator_deposit`)
and `1173744709` (`create_sub_account`) need the current protos: older protos
decode their payload as an unknown field, which the mapper's length guard (R2)
refuses.

The tests (`blocks/src/hypercore/value_tests.rs`) map every block with its true
identity and check row counts, labels, pinned values, the routing and the
populated-column matrix of each event table in `docs/chains/hypercore.md`, a
byte-exact rebuild of each payload from the raw tables (the union of the five
event tables included), golden derived rows, an independent naive re-derivation
of every derived value, and a pinned hash of the whole output (the release
invariant). Between them the fixtures give every one of the twelve tables rows:
16 outcome fills, 19 liquidated legs (4 `market`, 12 backstop takeovers, 3 ADL)
and 225 `funding_rates` rows.
`blocks/src/schema_contract_tests.rs` maps synthetic derivatives of them: block
headers rewritten to the harness identity, and the funding block cut to 8 fills
and 3 deltas per funding event.

## Checksums

| File | Block | Bytes | SHA-256 (payload) |
|---|---|---|---|
| `846001240.pb` | 846001240 | 308 | `04b24e181f7eb4e5c2e9c59483ab7286e71db7dd0b54466868410efb9c1d193a` |
| `846903317.pb.zst` | 846903317 | 12,362,099 | `1d791d3f698c2fe24db9d9c9200897bf189c3f1b18e2252ca07374a5d26dd384` |
| `847193990.pb` | 847193990 | 250 | `978bbf4788bfdaec645d9d6e12a8ad64522cb74cc4d9aa145e8697802d7f6cfa` |
| `889872017.pb` | 889872017 | 123 | `e5820932be07f996fcee3f679462398c1237e09b296a844391c610a435104a36` |
| `895702803.pb` | 895702803 | 247 | `74a15f76221bca0c54dd8dac3f18d2c833f36ff69cb561733d16ee5141661f5e` |
| `897888967.pb` | 897888967 | 494 | `92e85d3ffd72869eda96b69e31df2383b00d4fce8be57c40bd1e7cd03f4d7053` |
| `987247825.pb` | 987247825 | 1,273 | `b1dadb0349ab58e91207a81f4cbc6c8508aee3ed1f09e758c577f0af751c151e` |
| `1009557224.pb` | 1009557224 | 319 | `c3222cedc4bc122a38346aa4c0da5dd770aefc1fdab9fece80101def999dcfc2` |
| `1009612597.pb` | 1009612597 | 172 | `0c53f36b90e7c6b09d45b99ed5eea5f3cf46b49982e6603ac1517a514ee4683a` |
| `1009686466.pb` | 1009686466 | 149 | `9b0237f8987391e38c6f8c4b15a40b586f294cd1ff36251cef36a0726633d221` |
| `1009701302.pb` | 1009701302 | 279 | `3f40dc5cc6e6cc50d15b5c7e633cb3a9fb05ad0eedc657dcbf51a4dd5d5efe61` |
| `1009721907.pb` | 1009721907 | 193 | `09c438b3de1c864c6973211de45a023cc8271ef2e864ddd72d06eae8a8965936` |
| `1009855075.pb` | 1009855075 | 43,932 | `85756249bb015c400690cd644b70f26938cabc087b59de549521710d92d69856` |
| `1009867965.pb` | 1009867965 | 125 | `7ed7e35146cb45c688ace3b5f6c351fc584072e190d18e47a26d3c562e3f8c1b` |
| `1009868295.pb` | 1009868295 | 118 | `294b51e6c256719ec68de15f37dcb97dcc85f995b34bcc6693e8ae32748ed5c4` |
| `1009877929.pb` | 1009877929 | 101 | `e32a23415ffa8516234ca74f0f6d241bdb48f6b5b0c006d01ec5cef07990cf04` |
| `1009907496.pb` | 1009907496 | 208 | `3b323dd3c4d88df39fdb62e681abf98030440ee958e0ecf439e8de2f8c4f15d6` |
| `1009925229.pb` | 1009925229 | 115 | `20e8d9c70899ec1852b38155d317e9c6024101a0810aeda897e15500fdcdeedd` |
| `1009958482.pb` | 1009958482 | 123 | `53c969583fb5204f81c6bed2a0b61bc4e57997115c05253561c31562a1f951ad` |
| `1010128732.pb` | 1010128732 | 114 | `edf881c973711d703e4d5baef94c0100b25edc97e005e6f0aca100ac407fc190` |
| `1010355937.pb` | 1010355937 | 286 | `ba4b7f98f20067243de2b8b81bee6e49b2927d60524f0c22fe45c0356cad37bd` |
| `1010423738.pb` | 1010423738 | 177 | `a342f4d918eec4c8ecf73bd1e757d1f10f058fc8fa01e126113626522e9a2490` |
| `1010581248.pb` | 1010581248 | 3,286 | `748996726c74924a3b3841a9b4ca95dfdea6ac974b2d61f7f78235f764cc929e` |
| `1075395014.pb` | 1075395014 | 469 | `afc4bd941bb7890ccaccfb8453362cbe179e6b134ba0b911a988cdfb9d213b64` |
| `1075987296.pb` | 1075987296 | 4,550 | `1e426a29b7707616a16236a7cd056303685fb428787161fe48a716bd4e8379c1` |
| `1078677210.pb` | 1078677210 | 427 | `dddba53e5e401198db6df864866b249803474b426e7aff562fe8af6e81d13de8` |
| `1110656252.pb` | 1110656252 | 301 | `21dd8efb5b38ce96d3915cd88a88ef8f57a1ff7ee5b0d384bd42dcfb68122f01` |
| `1127672017.pb` | 1127672017 | 8,439 | `f7fc5d750b73f9201b90e41dbe85d4480ccc06d148f9aa9cfbd602b25fee82a3` |
| `1165601237.pb` | 1165601237 | 4,893 | `648c6df0649e81e20d382cb6779f930ba0e7f0d6a955e59fd3f2ba5cca899011` |
| `1173346041.pb` | 1173346041 | 131 | `2804546589a3b9506614b4432bdaa2c06d66442d2f157447e8ccc1e83154405e` |
| `1173352606.pb` | 1173352606 | 127 | `1a5466b65748997b679dd7900f7509abf9e0df81525cff9c24cf09703a0be9e4` |
| `1173408840.pb` | 1173408840 | 1,113 | `a8734ee78a21709e9ebec07b11e2a03db7d4b2e40d50340182914ee9bfe67745` |
| `1173546257.pb` | 1173546257 | 511 | `d8b22db87b6f55173d509d182b15e3532a6c54d1138b52591c0e63520d0dae0a` |
| `1173674198.pb` | 1173674198 | 453 | `c63b3a5933356d5a075e967843048c07d104887ac6e064455436472d7c13ec8d` |
| `1173744709.pb` | 1173744709 | 124 | `29987f84c278dc2c629e8c94ed8434dd7cdd3442f89fcf784ccbe36814b21fa7` |
| `1173886256.pb` | 1173886256 | 126 | `820c09d43e8b6d984ab4c67a5f3c79044bf6b35aa2ba6a7e0b898f2c8c0368cd` |

## Contents

| block | bytes | block_time (UTC) | fills | events | event bodies | ledger deltas | fill directions | fill flags |
|---|---|---|---|---|---|---|---|---|
| `846903317.pb.zst` | 12,362,099 | 2026-01-01T00:00:00.063624911Z | 1527 | 7 | funding×6, validator_rewards×1 (funding deltas total 202,449) | — | BUY×36, SELL×36, SPOT_DUST_CONVERSION×1451, CLOSE_SHORT×1, OPEN_SHORT×1, OPEN_LONG×1, CLOSE_LONG×1 | cloid×15, zero_hash×1523, coin:pair/×31, tid0×1451, fee_token!=USDC×30, coin:@×1492, coin:perp×4 |
| `1127672017.pb` | 8,439 | 2026-08-29T06:07:37.992763473Z | 38 | 12 | ledger_update×12 | liquidation×12 | CLOSE_SHORT×3, CLOSE_LONG×7, OPEN_LONG×4, LIQUIDATED_ISOLATED_LONG×18, LIQUIDATED_CROSS_LONG×6 | priority_gas×1, deployer_fee×4, cloid×14, coin:dex:×4, coin:perp×34, builder×1, liq:backstop×24 |
| `1173546257.pb` | 511 | 2026-10-06T06:44:38.005349546Z | 0 | 3 | ledger_update×3 | send×1, account_activation_gas×1, spot_transfer×1 | — | — |
| `1165601237.pb` | 4,893 | 2026-09-29T16:43:05.140125798Z | 28 | 1 | ledger_update×1 | borrow_lend×1 | OPEN_LONG×3, OPEN_SHORT×6, BUY×2, SELL×4, CLOSE_SHORT×7, SHORT_TO_LONG×1, CLOSE_LONG×4, LONG_TO_SHORT×1 | coin:perp×22, priority_gas×4, cloid×23, fee_token!=USDC×2, coin:@×4, builder×3, deployer_fee×2, coin:#×2 |
| `897888967.pb` | 494 | 2026-02-18T04:25:57.095001871Z | 2 | 2 | ledger_update×1, c_withdrawal×1 | c_staking_transfer×1 | OPEN_LONG×1, OPEN_SHORT×1 | cloid×1, zero_hash×2, coin:perp×2, twap×1 |
| `987247825.pb` | 1,273 | 2026-05-07T09:48:00.027671757Z | 0 | 3 | gossip_priority_auction_restart×2, validator_rewards×1 | — | — | — |
| `1010581248.pb` | 3,286 | 2026-05-25T23:04:49.349156022Z | 16 | 0 | — | — | OPEN_LONG×1, CLOSE_LONG×1, LIQUIDATED_ISOLATED_SHORT×3, AUTO_DELEVERAGING×3, CLOSE_SHORT×4, OPEN_SHORT×4 | cloid×5, coin:perp×2, liq:backstop×6, coin:dex:×14, liq:market×8, deployer_fee×8, builder×2 |
| `1009958482.pb` | 123 | 2026-05-25T11:11:59.200300118Z | 0 | 1 | delegation×1 | — | — | — |
| `895702803.pb` | 247 | 2026-02-16T02:28:07.273531878Z | 0 | 2 | ledger_update×2 | account_class_transfer×1, vault_create×1 | — | — |
| `846001240.pb` | 308 | 2025-12-31T03:32:35.244935172Z | 0 | 3 | ledger_update×3 | deposit×1, withdraw×2 | — | — |
| `1009557224.pb` | 319 | 2026-05-25T03:32:59.536554249Z | 0 | 2 | ledger_update×2 | vault_leader_commission×1, vault_withdraw×1 | — | — |
| `1173408840.pb` | 1,113 | 2026-10-06T04:00:36.587734253Z | 6 | 1 | ledger_update×1 | send×1 | CLOSE_SHORT×1, CLOSE_LONG×1, SETTLEMENT×4 | deployer_fee×3, cloid×2, coin:dex:×2, coin:#×4 |
| `1009877929.pb` | 101 | 2026-05-25T09:40:00.256212603Z | 0 | 1 | c_withdrawal×1 | — | — | — |
| `1010128732.pb` | 114 | 2026-05-25T14:26:50.938998904Z | 0 | 1 | ledger_update×1 | gossip_priority_gas_auction×1 | — | — |
| `1009925229.pb` | 115 | 2026-05-25T10:34:00.124738282Z | 0 | 1 | ledger_update×1 | rewards_claim×1 | — | — |
| `1009868295.pb` | 118 | 2026-05-25T09:28:55.651062654Z | 0 | 1 | ledger_update×1 | activate_dex_abstraction×1 | — | — |
| `889872017.pb` | 123 | 2026-02-10T13:03:20.246885946Z | 0 | 1 | ledger_update×1 | spot_genesis×1 | — | — |
| `1173744709.pb` | 124 | 2026-10-06T10:39:57.098539782Z | 0 | 1 | create_sub_account×1 | — | — | — |
| `1009867965.pb` | 125 | 2026-05-25T09:28:33.169875013Z | 0 | 1 | delegation×1 | — | — | — |
| `1173886256.pb` | 126 | 2026-10-06T13:28:47.040544066Z | 0 | 1 | ledger_update×1 | borrow_lend×1 | — | — |
| `1173352606.pb` | 127 | 2026-10-06T02:54:25.089535269Z | 0 | 1 | ledger_update×1 | borrow_lend×1 | — | — |
| `1173346041.pb` | 131 | 2026-10-06T02:46:35.236833275Z | 0 | 1 | ledger_update×1 | borrow_lend×1 | — | — |
| `1009686466.pb` | 149 | 2026-05-25T06:00:46.104049222Z | 0 | 1 | ledger_update×1 | vault_deposit×1 | — | — |
| `1009612597.pb` | 172 | 2026-05-25T04:36:15.167113660Z | 0 | 1 | ledger_update×1 | sub_account_transfer×1 | — | — |
| `1010423738.pb` | 177 | 2026-05-25T20:04:53.118187651Z | 0 | 1 | ledger_update×1 | internal_transfer×1 | — | — |
| `1009721907.pb` | 193 | 2026-05-25T06:41:23.261996850Z | 0 | 2 | ledger_update×1, c_deposit×1 | c_staking_transfer×1 | — | — |
| `1009907496.pb` | 208 | 2026-05-25T10:13:41.106613278Z | 0 | 1 | ledger_update×1 | send×1 | — | — |
| `847193990.pb` | 250 | 2026-01-01T06:34:26.361728636Z | 0 | 2 | ledger_update×2 | vault_distribution×2 | — | — |
| `1009701302.pb` | 279 | 2026-05-25T06:17:48.218987173Z | 2 | 0 | — | — | SPLIT_OUTCOME×2 | fee_token!=USDC×2, coin:#×2 |
| `1010355937.pb` | 286 | 2026-05-25T18:46:56.082641887Z | 2 | 0 | — | — | MERGE_OUTCOME×2 | coin:#×2 |
| `1110656252.pb` | 301 | 2026-08-15T05:22:41.943263479Z | 2 | 0 | — | — | NET_CHILD_VAULTS×2 | coin:perp×2 |
| `1078677210.pb` | 427 | 2026-07-19T18:36:46.194396314Z | 3 | 0 | — | — | NEGATE_OUTCOME×3 | coin:#×3, fee_token!=USDC×2 |
| `1173674198.pb` | 453 | 2026-10-06T09:16:32.765999452Z | 2 | 1 | ledger_update×1 | deploy_gas_auction×1 | CLOSE_SHORT×1, CLOSE_LONG×1 | cloid×2, coin:perp×2 |
| `1075395014.pb` | 469 | 2026-07-17T01:33:03.238272114Z | 3 | 0 | — | — | MERGE_QUESTION×3 | coin:#×3 |
| `1075987296.pb` | 4,550 | 2026-07-17T13:18:02.001797038Z | 26 | 1 | ledger_update×1 | hip3_liquidator_deposit×1 | OPEN_LONG×7, CLOSE_LONG×12, CLOSE_SHORT×6, OPEN_SHORT×1 | twap×4, zero_hash×8, coin:perp×14, cloid×22, deployer_fee×12, coin:dex:×12 |
| `1009855075.pb` | 43,932 | 2026-05-25T09:13:48.967459338Z | 314 | 0 | — | — | SETTLEMENT×314 | coin:perp×314, zero_user×157 |
