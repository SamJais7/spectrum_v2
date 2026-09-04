from main import load_config

cfg = load_config()
assert cfg['x'].get('bearer_token'), 'X_BEARER_TOKEN missing'
assert cfg['telegram'].get('api_id') and cfg['telegram'].get('api_hash'), 'Telegram creds missing'
print('✓ config + .env load correctly')
print('  X mode:', cfg['x'].get('mode'))
print('  TG targets:', cfg['telegram']['targets'])