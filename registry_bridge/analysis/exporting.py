"""登记量分析结果的文件导出与导回，保证已发版本可原样取回。"""
import json
from pathlib import Path

from registry_bridge.analysis import arrivals
from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import canonical
from registry_bridge.query.exporting import atomic_write

ALLOWED_KINDS = frozenset({'arrival_analysis', 'arrival_hindcast'})


def write_analysis(store, analysis_id, path):
    from registry_bridge.analysis.registry import get_analysis
    document = get_analysis(store, analysis_id)
    if arrivals.document_digest(document) != analysis_id:
        raise InvalidMessage('Stored analysis failed digest verification')
    envelope = {'format': 'registry-arrival-analysis', 'version': 1, 'document': document}
    content = canonical(envelope) + '\n'
    result = atomic_write(path, content)
    return {**result, 'analysis_id': analysis_id, 'kind': document['kind']}


def read_analysis_file(path, *, maximum=64 * 1024 * 1024):
    file = Path(path)
    if not file.is_file() or file.is_symlink():
        raise InvalidMessage('Analysis export must be a regular non-symlink file')
    if file.stat().st_size > maximum:
        raise InvalidMessage('Analysis export exceeds size limit')
    try:
        envelope = json.loads(file.read_text(encoding='utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise InvalidMessage('Analysis export must contain UTF-8 JSON') from exc
    return verify_envelope(envelope)


def verify_envelope(envelope):
    if not isinstance(envelope, dict) or set(envelope) != {'format', 'version', 'document'}:
        raise InvalidMessage('Analysis envelope fields do not match schema')
    if envelope['format'] != 'registry-arrival-analysis' or envelope['version'] != 1:
        raise InvalidMessage('Unsupported analysis export format')
    document = envelope['document']
    if not isinstance(document, dict) or document.get('kind') not in ALLOWED_KINDS:
        raise InvalidMessage('Analysis document is missing a supported kind')
    analysis_id = document.get('analysis_id')
    if analysis_id != arrivals.document_digest(document):
        raise InvalidMessage('Analysis document digest mismatch')
    return document


def import_analysis(store, path):
    """把导出文件原样写回台账；已存在的同 id 版本保持不变。"""
    from registry_bridge.analysis.registry import save_analysis
    document = read_analysis_file(path)
    saved = save_analysis(store, document)
    return {**saved, 'kind': document['kind']}
