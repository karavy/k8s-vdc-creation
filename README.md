# k8s-vdc-creation

## Moduli condivisi

L'operator utilizza due moduli Go locali, tramite le direttive `replace` in
[go.mod](go.mod). I checkout devono essere in directory sorelle del repository:

- `github.com/karavy/terraform-engine`: directory sorella `terraform-engine`,
	usata da `ApplyTerraformRule`.
- `github.com/karavy/api-engine`: directory sorella `api-engine`, package
	`github.com/karavy/api-engine/pkg/apiengine`, usato da `ApplyAPIRequest`.

Il manager inizializza un solo client API in [cmd/main.go](cmd/main.go) e lo
inietta nel controller in
[internal/controller/basecluster_controller.go](internal/controller/basecluster_controller.go).
Il client supporta GET, PUT, POST e DELETE, propaga il contesto della chiamata e
invia il body senza serializzazioni aggiuntive. La configurazione predefinita
prevede un timeout di 30 secondi, risposte limitate a 10 MiB, nessun retry
applicativo e nessun redirect automatico.

`ApplyAPIRequest` restituisce la risposta del modulo senza convertire gli status
HTTP 4xx/5xx in errori Go: il chiamante deve verificare `Response.IsSuccess()` e
gestire gli errori prima di utilizzare eventuali risposte parziali. Endpoint e
destinazioni devono essere autorizzati dall'operator; le credenziali devono
provenire da Secret e non vanno registrate nei log.

La riconciliazione di `BaseCluster` è ancora uno scheletro: questa integrazione
rende disponibile il client, ma non esegue chiamate automaticamente e non
aggiunge campi alla CRD. Il mapping delle operazioni sulle risorse esterne resta
da definire.

## ApiCall

I file Kubebuilder [internal/controller/apicall_controller.go](internal/controller/apicall_controller.go)
e [internal/controller/apicall_controller_test.go](internal/controller/apicall_controller_test.go)
restano nella cartella principale dei controller e usano la suite envtest comune.
La logica di supporto, l'executor HTTP, la preparazione delle richieste e i test
unitari sono raccolti nel package `internal/controller/apicall`.

La CRD [ApiCall](api/v1alpha1/apicall_types.go) rappresenta una singola esecuzione
di una chiamata HTTP tramite `api-engine`. Il manifest completo è in
[config/samples/kmetal_v1alpha1_apicall.yaml](config/samples/kmetal_v1alpha1_apicall.yaml).

**Un ApiCall creato da solo non viene mai eseguito.** La definizione riusabile di
una chiamata è un [ApiCallTemplate](api/v1alpha1/apicalltemplate_types.go) (metodo,
endpoint, body, header, autenticazione, timeout), che non ha status né controller
e non viene mai eseguito. Un [Workflow](api/v1alpha1/workflow_types.go) lo richiama
per nome con `templateRef`: per ogni step crea un ApiCall figlio, di cui è owner, ed
è solo questo che il controller esegue. Lo stesso template può quindi essere riusato
da più workflow e più step. Poll e passaggio dei dati (`inputs`/`outputs`) si
definiscono nello step del Workflow; anche il poll richiama un template per nome.
Esempi in
[config/samples/kmetal_v1alpha1_apicalltemplate.yaml](config/samples/kmetal_v1alpha1_apicalltemplate.yaml)
e [config/samples/kmetal_v1alpha1_workflow.yaml](config/samples/kmetal_v1alpha1_workflow.yaml).

Per le API asincrone lo step ha un `poll`: dopo la chiamata iniziale ripete a
`intervalSeconds` (default 5) la chiamata del template `poll.templateRef`, fino a
`maxAttempts` (default 20), finché `conditionJSONPointer` nella risposta non vale
`equalsJSON`. Se il poll non è pronto entro `maxAttempts` il workflow fallisce con
`PollTimeout`. Di default qualsiasi poll con HTTP non 2xx, timeout o errore di rete
fa fallire il workflow; per tollerarli si dichiara `retryOnStatusCodes` (per esempio
`[404, 503]`) e/o `retryOnTransportErrors: true`. Un poll tollerato conta come "non
pronto" e consuma un tentativo; richieste non valide, risposte troppo grandi ed
esecuzioni perse non sono mai tollerate, e le opzioni non valgono per la chiamata
iniziale.

### Passare dati da uno step all'altro

Uno step pubblica valori **con nome** dalla propria risposta JSON (`outputs`) e
qualsiasi step successivo, non solo il successivo immediato, li inserisce nel body
della propria richiesta (`inputs`):

```yaml
steps:
  - name: create-vdc
    api:
      templateRef: {name: create-vdc}
      outputs:
        - {name: vdcId, from: /id}                          # JSON Pointer nella risposta
        - {name: region, from: /region, defaultJSON: '"eu-1"'}
        - {name: token, from: /token, sensitive: true}
  - name: create-network
    api:
      templateRef: {name: create-network}
      inputs:
        - {step: create-vdc, output: vdcId, to: /vdcId}     # JSON Pointer nel body
```

- Se `from` non esiste nella risposta, il workflow **fallisce** (`OutputMissing`) e
  non invia mai `null` di nascosto. Un valore di ripiego va dichiarato in modo
  esplicito con `defaultJSON` (qualsiasi JSON, anche `null`).
- I riferimenti sono validati **prima di eseguire qualsiasi step**: step sconosciuto,
  step successivo, output non dichiarato o nomi duplicati portano il workflow in
  `Failed` con reason `InvalidReference`, senza inviare richieste.
- I valori non sensibili sono in `status.steps[].outputs` (JSON, al massimo 4 KiB
  l'uno e 32 per step). Gli output `sensitive: true` non compaiono mai nello status:
  stanno nel Secret `status.outputsSecretName` (owner il Workflow, chiave
  `<step>.<output>`), e lo status ne elenca solo i nomi in `sensitiveOutputs`.
- Con un `poll`, gli `outputs` dello step leggono la risposta **iniziale**;
  `poll.outputs` legge la risposta **finale** (quella che soddisfa la condizione), e
  `poll.inputs` può usare gli output dello step e di quelli precedenti.
- Lo step Terraform usa `inputs` allo stesso modo: `to` è il nome della variabile
  (`/cluster_id`) nel `terraform.auto.tfvars.json`.
- I valori si inseriscono solo nel **body JSON** (non in URL, query o header).

**Limite sui dati sensibili:** il Secret protegge lo status del Workflow e permette
un RBAC separato, ma il valore, una volta usato in un `input`, finisce nel body
dell'ApiCall figlio, e la risposta completa resta in `status.response` di ogni
ApiCall figlio (il Workflow richiede `storeResponseBody: true` per poterla leggere).
Chi può leggere gli ApiCall del namespace può quindi vedere quei valori.

### Configurazione

- `spec.method`: GET, PUT, POST o DELETE.
- `spec.endpoint`: URL assoluto HTTP(S), inclusi eventuali parametri di query.
	È l'unico campo della spec modificabile dopo la creazione. Cambiarlo retargetta
	esplicitamente la chiamata; tutti gli altri campi della spec sono immutabili.
- `spec.body`: stringa inviata **senza serializzazione aggiuntiva**, massimo
	64 KiB. Per JSON, usare una stringa o un blocco YAML e specificare
	`Content-Type: application/json` in `spec.headers`.
- `spec.authentication`: opzionale, con `type: Bearer`, `Basic`, `APIKey` o
  `OAuth2ClientCredentials`.
	Bearer e APIKey usano `tokenSecretRef: {name, key}`; Basic usa
	`usernameSecretRef` e `passwordSecretRef`; APIKey richiede anche `headerName`
	(per esempio `X-API-Key`). OAuth2 richiede `tokenURL`, `clientIDSecretRef` e
	`clientSecretSecretRef`; supporta `clientAuthenticationMethod` uguale a
	`ClientSecretBasic` (default) o `ClientSecretPost`, scopes facoltativi e il
	parametro provider-specific `audience`. L'operator richiede un access token
	con grant `client_credentials` prima della richiesta API, usando lo stesso
	timeout complessivo. `ClientSecretBasic` applica la form-encoding OAuth alle
	credenziali prima dell'header Basic; `ClientSecretPost` le invia nel body TLS.
	Il token Bearer viene usato solo per la chiamata API, non è messo in cache o
	scritto nello stato.
	Token endpoint e API devono entrambi avere origini HTTPS autorizzate.
	Tutti i Secret devono essere nella **stessa namespace** della CR. Le
	credenziali e i token sono inviati solo via HTTPS con verifica TLS. I certificati
	remoti devono essere riconosciuti dal trust store del manager.
- `spec.timeoutSeconds`: default 120, intervallo 1–3600. Il timeout parte quando
	un worker avvia la richiesta e copre anche la lettura della risposta; il tempo
	trascorso in coda non è incluso.
- `spec.storeResponseBody`: default true. Impostare false se la risposta è
	sensibile: il body non verrà salvato nello stato.
- `spec.executionID`: stringa opzionale che distingue ApiCall altrimenti identici.
	La spec è immutabile, quindi non serve a rieseguire un ApiCall esistente.

Il manager espone questi flag:

| Flag | Default | Descrizione |
| --- | --- | --- |
| `--apicall-allowed-origins` | vuoto | Origini esatte separate da virgole, ad esempio `https://api.example.com,https://other.example.com:8443`. Vuoto **nega tutte le chiamate**. |
| `--apicall-workers` | 4 | Massimo numero di richieste HTTP concorrenti, 1–64. |
| `--apicall-queue-size` | 64 | Capacità aggiuntiva del pool, 1–4096. I risultati completati ma non ancora persistiti occupano capacità. |

Per il manifest OAuth2 di esempio, configurare
`--apicall-allowed-origins=https://api.example.com,https://auth.example.com`
negli argomenti del manager in [config/manager/manager.yaml](config/manager/manager.yaml)
e creare il Secret `oauth-client-credentials` con le chiavi `client-id` e
`client-secret` nella namespace della CR. Nessun Secret con credenziali reali
viene incluso nei manifest di esempio.

### Esecuzione e stato

Il [controller](internal/controller/apicall_controller.go) delega la preparazione e
la gestione dello stato ai [helper](internal/controller/apicall/reconcile.go) e
salva il checkpoint `Running` **prima** di inviarla. Il
[pool di worker](internal/controller/apicall/api_executor.go) è un `manager.Runnable`
leader-only: le goroutine partono da `Start`, non da `Reconcile`, e sono cancellate
e attese alla chiusura del manager. Anche richieste sincrone lente non occupano un
worker di riconciliazione. Non viene creato un processo OS separato.
Per più repliche è necessario abilitare la leader election, come già previsto dal
Deployment di esempio; senza leader election più manager possono inviare duplicati.

Un evento di completamento rimette la CR nella coda del controller. Un controllo
ogni 2 secondi funge da fallback. Solo il controller aggiorna lo stato; se il
salvataggio fallisce, il risultato resta in memoria per il tentativo successivo.
Il pool limita richieste, coda e risultati in memoria. Quando è pieno, le altre CR
restano `Pending`. Un Secret assente viene ricontrollato ogni 10 secondi, senza
avviare HTTP e senza inserire valori sensibili nelle condizioni. Ogni lettura di
un Secret è limitata a 5 secondi, o alla deadline più breve del contesto.
Le credenziali sono fotografate al momento della preparazione della richiesta:
una rotazione del Secret non modifica le operazioni già in coda o in esecuzione
e non ripete quelle completate. Per OAuth2, l'access token viene richiesto dal
worker immediatamente prima della chiamata protetta e non viene memorizzato in
cache né nello stato. Un nuovo ApiCall acquisisce i valori correnti dei
Secret. Per ripetere l'operazione, dopo aver verificato l'effetto remoto, creare
un nuovo ApiCall (per esempio lanciando un nuovo Workflow).

| Fase | Significato |
| --- | --- |
| `Pending` | In attesa di credenziali o capacità del pool. |
| `Running` | Checkpoint persistito; richiesta in coda o in esecuzione. |
| `Succeeded` | Risposta HTTP 2xx completa ricevuta. |
| `Failed` | Configurazione non ammessa, risposta non-2xx, timeout, errore di trasporto/lettura o risposta troppo grande. |
| `Unknown` | Stato del worker perso dopo un riavvio/failover; l'effetto remoto non è noto. |

`status` espone `observedGeneration`, `startedAt`, `completedAt`, condizioni
`Ready`/`Progressing` e `response` con `statusCode`, `contentType`, `body` e
`truncated`. `response.body` è **base64** per conservare anche dati binari;
decodificandolo si ottiene il payload originale. Le risposte sono limitate a
64 KiB; se il limite è superato, `Failed/ResponseTooLarge` e `truncated: true`
indicano che il body salvato è parziale e non deve essere usato come risultato
completo. Le risposte HTTP di errore sono conservate, mentre gli altri header
remoti (potenzialmente sensibili, ad esempio `Set-Cookie`) non vengono salvati.

Un `202 Accepted` conclude la **chiamata HTTP**, non necessariamente il lavoro
remoto. Il polling di un job remoto richiede un protocollo specifico e non è
implementato implicitamente.

### Idempotenza, riavvii e sicurezza

- La spec di un ApiCall è **interamente immutabile** (`self == oldSelf`): l'API
	Kubernetes rifiuta qualsiasi modifica dopo la creazione, `spec.endpoint`
	compreso. Un ApiCall è la traccia di una singola esecuzione; per una richiesta
	diversa serve un nuovo ApiCall, normalmente creato da un Workflow a partire da un
	ApiCallTemplate (che invece resta modificabile). Riconciliazioni, aggiornamenti
	dello stato e modifiche dei metadati non ripetono la chiamata.
- Non ci sono retry applicativi automatici né redirect. Il transport Go può
	ritentare autonomamente alcune richieste idempotenti su connessioni riutilizzate.
	Usare un `Idempotency-Key` stabile negli header solo se l'API remota garantisce
	deduplicazione, e scegliere una nuova chiave per un'operazione nuova.
- Un riavvio con una CR `Running` produce `Unknown`, **non un replay**. Anche
	un crash prima dell'invio ma dopo il checkpoint può produrre `Unknown`.
	Senza un protocollo di idempotenza remoto non è possibile garantire
	exactly-once né ricostruire un risultato HTTP perso. Verificare l'effetto
	remoto prima di creare un nuovo ApiCall per riprovare.
- Cancellazione della CR, nuova generazione e arresto del manager annullano
	l'attesa locale. Questo **non annulla** effetti già avvenuti sul server.
	Nessun finalizer è aggiunto: una richiesta HTTP non può garantire un rollback
	remoto. Un nuovo tentativo può sovrapporsi a un'operazione ancora attiva sul server.
- `status` non è un Secret. Il body remoto potrebbe contenere credenziali o dati
	personali: usare `storeResponseBody: false` e restringere il RBAC delle CR.
	Non mettere credenziali in endpoint, body o header pubblici; usare Secret.
- La allowlist autorizza schema, host e porta, ma **non blocca DNS rebinding** o
	cambiamenti degli IP risolti. Consentire soltanto origini fidate e usare una
	NetworkPolicy/egress policy per bloccare metadata cloud e destinazioni vietate.
	Una origine autorizzata autorizza tutti i suoi percorsi.
- L'operator ha RBAC `get` sui Secret. Chi può creare ApiCall può chiedergli di
	utilizzare un Secret della stessa namespace verso una destinazione autorizzata.
	Limitare queste autorizzazioni a utenti e namespace fidati; l'allowlist non
	sostituisce il controllo degli accessi ai Secret.

CRD, RBAC e DeepCopy sono generati con `make manifests generate`. `make test`
verifica schema CRD, esecuzione non bloccante, autenticazione TLS, metodi HTTP,
timeout, cancellazione, limiti della risposta, deduplicazione e recupero `Unknown`.

## Mock API per debug in-cluster

Per verificare cosa invia l'operator, il repository include un server mock
indipendente in [cmd/mock-api/main.go](cmd/mock-api/main.go) e il deployment
opzionale in [config/debug-api](config/debug-api). Il server registra le ultime
100 richieste in memoria e restituisce i dettagli della richiesta. Gli header di
autenticazione, cookie, API key, token e campi sensibili nei body JSON/form sono
oscurati. Le richieste non persistono tra riavvii; `DELETE /__debug/requests`
cancella lo storico.

### Avvio locale

Usare `make mock-api-test` per i test, `make mock-api-run` per ascoltare su
`localhost:8080`, oppure `make mock-api-image` per creare l'immagine
`k8s-vdc-creation-mock-api:dev`. Endpoint utili:

| Endpoint | Comportamento |
| --- | --- |
| `GET /healthz` | Readiness/liveness. |
| Qualsiasi metodo, per esempio `/echo` | Registra e restituisce metodo, path, query, header e body. |
| `POST /oauth/token` | Simula OAuth Client Credentials e restituisce un token fittizio. Registra la richiesta oscurando client secret e header Basic. |
| Qualsiasi metodo `/status/401` (o altro status 200–599) | Simula una risposta HTTP con quello status e include la richiesta catturata. |
| `GET /__debug/requests?limit=20` | Restituisce le ultime richieste acquisite, fino a 100. |
| `DELETE /__debug/requests` | Svuota lo storico. |

### Avvio nel cluster

1. Costruire e caricare l'immagine nel cluster locale (per esempio Kind):

	 ```sh
	 make mock-api-image
	 kind load docker-image k8s-vdc-creation-mock-api:dev
	 make debug-api-deploy
	 ```

	 Per un cluster remoto, pubblicare l'immagine in un registry raggiungibile e
	 aggiornare `image` in `config/debug-api/mock-api.yaml`.

2. Per testare solo richieste **senza credenziali**, autorizzare l'origine HTTP
	 sul manager, poi applicare la CR. Il DNS del Service è
	 `mock-api.apicall-debug.svc.cluster.local:8080` e il suo endpoint `/echo` è
	 raggiungibile dal pod operator. Esempio di CR da salvare come file temporaneo:

	 ```yaml
	 apiVersion: kmetal.dinova.one/v1alpha1
	 kind: ApiCallTemplate
	 metadata:
		 name: mock-echo
	 spec:
		 method: POST
		 endpoint: http://mock-api.apicall-debug.svc.cluster.local:8080/echo
		 headers:
			 Content-Type: application/json
			 X-Debug-Value: hello
		 body: '{"message":"test da ApiCall"}'
	 ---
	 apiVersion: kmetal.dinova.one/v1alpha1
	 kind: Workflow
	 metadata:
		 name: mock-echo-run
	 spec:
		 steps:
			 - name: echo
				 api:
					 templateRef:
						 name: mock-echo
	 ```

	 Aggiungere `--apicall-allowed-origins=http://mock-api.apicall-debug.svc.cluster.local:8080`
	 agli argomenti del manager (oltre a `--leader-elect` e ai normali argomenti
	 esistenti), quindi applicare le CR. Il template `mock-echo` da solo non fa nulla;
	 la chiamata parte con il Workflow. Leggere risultato e fase della copia figlia
	 con `kubectl get apicall -l kmetal.dinova.one/workflow=mock-echo-run -o yaml`.

3. **Autenticazione e OAuth richiedono HTTPS anche nei test.** Generare una CA/
	 certificato effimero con SAN del Service; non disattivare la verifica TLS:

	 ```sh
	 openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
		 -keyout /tmp/mock-api.key -out /tmp/mock-api.crt \
		 -subj '/CN=mock-api.apicall-debug.svc.cluster.local' \
		 -addext 'subjectAltName=DNS:mock-api.apicall-debug.svc.cluster.local'
	 kubectl create secret tls mock-api-tls -n apicall-debug \
		 --cert=/tmp/mock-api.crt --key=/tmp/mock-api.key
	 kubectl rollout restart deployment/mock-api -n apicall-debug
	 ```

	 Lo stesso certificato autofirmato può essere usato come CA di test. Creare
	 un ConfigMap nel namespace del manager e applicare la patch pronta all'uso:

	 ```sh
	 kubectl create configmap apicall-debug-ca -n k8s-vdc-creation-system \
	   --from-file=ca.crt=/tmp/mock-api.crt
	 kubectl patch deployment k8s-vdc-creation-controller-manager \
	   -n k8s-vdc-creation-system --type=strategic \
	   --patch-file=config/debug-api/operator-tls-patch.yaml
	 ```

	 La patch monta la CA e configura allowlist e `--apicall-ca-file` senza
	 disabilitare la verifica TLS. Applicare poi il CR OAuth di esempio in
	 [config/debug-api/apicall-oauth-sample.yaml](config/debug-api/apicall-oauth-sample.yaml).
	 Il manifest usa solo credenziali fittizie ed entrambi i passaggi
	 `/oauth/token` e `/echo` sono visibili nello storico.

4. Per ispezionare le richieste dall'ambiente locale, fare port-forward al
	 Service e interrogare `GET /__debug/requests`. Con il certificato di test,
	 `curl` locale deve usare `-k` **solo per il port-forward di debug**; questo non
	 cambia la verifica TLS dell'operator. In alternativa leggere i log con
	 `kubectl logs -n apicall-debug deployment/mock-api`.

Il mock è HTTP senza certificati e passa a HTTPS quando trova il Secret TLS
opzionale. È uno strumento di sviluppo, senza autenticazione sull'interfaccia di
debug e con risposte/catture in memoria: distribuirlo solo in cluster di test,
non esporlo con Ingress/LoadBalancer e non usarlo in produzione. Anche con
redazione, inviare soltanto credenziali fittizie.

## Tilt: stack locale operatore + mock

Il [Tiltfile](Tiltfile) prepara lo stack completo sul cluster Kind locale
`kind-er-operator` (già verificato disponibile): CRD/RBAC/operator, mock API e
un ApiCallTemplate `tilt-mock-echo` e il Workflow `tilt-mock-echo-run` che lo richiama. Tilt costruisce **entrambe le immagini in
locale**; per l'operator il [Dockerfile Tilt](Dockerfile.tilt) usa i checkout
fratelli locali `../api-engine` e `../terraform-engine` tramite BuildKit, senza
registry remoto. Nessun Ingress o LoadBalancer viene creato.

Prerequisiti: Tilt, Docker/BuildKit, `kubectl`, cluster Kind attivo chiamato
`kind-er-operator`, e i due moduli condivisi presenti in `/code/api-engine` e
`/code/terraform-engine`. Dalla root del repository avviare:

```sh
tilt up --context kind-er-operator
```

Tilt mantiene la sessione aperta e mostra build, stato dei pod e log del mock e
dell'operator; l'interfaccia web si apre normalmente su
`http://localhost:10350`. La prima build scarica le immagini base Go e
Distroless; le immagini applicative sono costruite localmente e caricate nel
cluster Kind da Tilt. Gli aggiornamenti al sorgente ricostruiscono l'immagine e
aggiornano il deployment. Tilt crea inoltre un port-forward da
`localhost:8088` al mock per consultare lo storico delle richieste.

Il Workflow `tilt-mock-echo-run` crea un ApiCall dal template `tilt-mock-echo` e
invia una POST al Service interno (il template da solo non viene eseguito). Per controllare risultato e richiesta catturata:

```sh
kubectl get workflow tilt-mock-echo-run -o yaml
kubectl get apicall -l kmetal.dinova.one/workflow=tilt-mock-echo-run -o yaml
kubectl logs -n apicall-debug deployment/mock-api
```

Il Workflow `tilt-vdc-network` ([workflow-sequence.yaml](config/tilt/workflow-sequence.yaml))
esegue in sequenza due ApiCallTemplate distinti: `POST /vdcs` crea un VDC e restituisce
l'`id`, poi `POST /networks` riceve quell'`id` come `vdcId` tramite `outputs`/`inputs`. Il mock
risponde 404 se il `vdcId` non esiste, quindi l'ordine è verificato davvero:

```sh
kubectl get workflow tilt-vdc-network
kubectl get apicall -l kmetal.dinova.one/workflow=tilt-vdc-network
```

Il body inviato e la risposta sono nello status della CR; lo storico redatto è
consultabile in `http://localhost:8088/__debug/requests` oppure dal pod mock.
Per fermare Tilt premere Ctrl+C; questo **lascia le risorse nel cluster** per
poter ispezionare lo stato.
Per eliminarle eseguire:

```sh
tilt down --context kind-er-operator
```

Il target Docker locale si può verificare separatamente con
`make mock-api-image`; Tilt usa invece l'immagine con tag `:tilt` per non
confonderla con il deployment debug manuale.
