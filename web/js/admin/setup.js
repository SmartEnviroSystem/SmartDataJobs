window['select_target_options'] = {
    showWhenNoData: true
};

window['adm_presets_options'] = {
    showWhenNoData: true
};

document.addEventListener('swac_select_target_selection_changed', async function (evt) {
    const source = evt.detail.value;
    const presetsElem = document.querySelector('#adm_presets');
    presetsElem.hidden = !source;
    if (!source) {
        return;
    }
    const url = '/' + source + '/smartdata/[iface]/[fromName]?storage=smartmonitoring';
    if (window['adm_presets_options'].targetSource?.url === url) {
        return;
    }
    window['adm_presets_options'].targetSource = {
        url: url,
        interfaces: {
            get: ['GET', 'records'],
            list: ['GET', 'records'],
            defs: ['GET', 'collection'],
            cdefs: ['POST', 'collection'],
            create: ['POST', 'records'],
            update: ['PUT', 'records'],
            delete: ['DELETE', 'records']
        }
    };

    if (!presetsElem.hasAttribute('swa')) {
        presetsElem.setAttribute('swa', presetsElem.dataset.swa);
        const { default: ViewHandler } = await import('/SWAC/swac/ViewHandler.js');
        await new ViewHandler().load(presetsElem);
    } else if (presetsElem.swac_comp?.options) {
        presetsElem.swac_comp.options.targetSource = window['adm_presets_options'].targetSource;
        presetsElem.swac_comp.reload();
    }
});
