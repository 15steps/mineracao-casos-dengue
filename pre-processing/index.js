const mongoose = require('mongoose')
const moment = require('moment')

mongoose.connect('mongodb://localhost/dengue_2018', {useNewUrlParser: true}, async (err) => {
    if (err) console.log('shit happens')
    else {
        const db = mongoose.connection
        const dengue_novo = db.collection('dengue_2018')
        const result = await dengue_novo.find()
        let i = 0
        result.on('data', doc => {
            transformDates(i, dengue_novo, doc)
            computeBirthday(i, dengue_novo, doc)
            // computeClass(i, dengue_novo, doc)
            i++;
        })
        result.on('end', () => console.log('query finished'))
    }
})

const transformDates = (ix, collection, doc) => {
    let dt_notificacao = ''
    let dt_diagnostico = ''
    if (doc.dt_notificacao.indexOf(':') > -1) {
        dt_notificacao = moment(doc.dt_notificacao, 'YYYY-MM-dd hh:mm:ss')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'YYYY-MM-dd hh:mm:ss')
    } else if (doc.dt_notificacao.indexOf('-') > -1) {
        dt_notificacao = moment(doc.dt_notificacao, 'dd-MM-YYYY')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'dd-MM-YYYY')
    } else {
        dt_notificacao = moment(doc.dt_notificacao, 'MM/DD/YY')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'MM/DD/YY')
    }
    const diff = dt_notificacao.diff(dt_diagnostico, 'days')
    collection.update({_id: doc._id}, {$set: {notificao_dias: Math.abs(diff)}}, {multi: true}, (err, _doc) => {
        if (err) console.log(err)
        else {
            console.log(`[${ix}] ${doc._id} was set to notificao_dias ${Math.abs(diff)}`)
        }
    })
}

const computeBirthday = (ix, collection, doc) => {
    let birthday = ''
    let dt_diagnostico = ''
    if (doc.dt_notificacao.indexOf(':') > -1) {
        birthday = moment(doc.dt_nascimento, 'YYYY-MM-dd hh:mm:ss')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'YYYY-MM-dd hh:mm:ss')
    } else if (doc.dt_notificacao.indexOf('-') > -1) {
        birthday = moment(doc.dt_nascimento, 'dd-MM-YYYY')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'dd-MM-YYYY')
    } else {
        birthday = moment(doc.dt_nascimento, 'MM/DD/YY')
        dt_diagnostico = moment(doc.dt_diagnostico_sintoma, 'MM/DD/YY')
    }
    const age = birthday.isValid() ? dt_diagnostico.diff(birthday, 'years') : null
    // if (age === 0) {
    //     // console.log(`birthday: ${birthday} dt_diagnostico: ${dt_diagnostico}`)
    // }
    collection.update({_id: doc._id}, {$set: {idade: Math.abs(age)}}, {multi: true}, (err, _doc) => {
        if (err) console.log(err)
        else {
            console.log(`[${ix}] ${doc._id} was set to age ${Math.abs(age)}`)
        }
    })
    // if (!birthday.isValid()) {
    //     console.log(`invalid birthday ${doc.dt_nascimento} ${doc.febre}`)
    // }
}

const YES = 'SIM'
const NO = 'NAO'
const comorbidades = ['hipertensao', 'auto_imune', 'acido_pept', 'hematolog', 'hepatopat', 'renal'];
const temMais2Comorbidades = doc => {
    return comorbidades
        .map(prop => doc[prop]) // get props
        .map(val => val === '1' ? 1 : 0) // transform to num
        .reduce((a, b) => a + b, 0) >= 2 // sum
}
const computeClass = (ix, collection, doc) => {
    let finalClass = ''
    const hasDengueGrave = ['2', '3', '4', '11', '12'].includes(doc.tp_classificacao_final)
    const hasDengue = ['1', '2', '3', '4', '10', '11', '12'].includes(doc.tp_classificacao_final)
    if (hasDengueGrave) {
        finalClass = YES
    } else if (hasDengue && ['1', '2', '3'].includes(doc.tp_gestante)) {
        // Gestação + Dengue
        finalClass = YES
    } else if (doc.idade >= 70 && hasDengue) {
        // 70 anos ou mais + Dengue
        finalClass = YES
    } else if (hasDengue && temMais2Comorbidades(doc)) {
        // 2 ou mais comorbidades + dengue
        finalClass = YES
    } else if (doc.dt_obito !== '') {
        // morreu
        finalClass = YES
    } else if (doc.sangram === '1') {
        // hemorragia
        finalClass = YES
    } else if (doc.metro === '1') {
        // homorragia
        finalClass = YES
    } else if (!['', '8'].includes(doc.complica)) {
        // teve alguma complicacao
        finalClass = YES
    } else if (doc.dt_internacao !== '') {
        // foi internado
        finalClass = YES
    } else {
        finalClass = NO
    }
    collection.update({_id: doc._id}, {$set: {caso_internacao: finalClass}}, {multi: true}, (err, _doc) => {
        if (err) console.log(err)
        else {
            console.log(`[${ix}] ${doc._id} was set to caso_internacao ${finalClass}`)
        }
    })
}