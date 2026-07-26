// ---------------------------------------------------------------------------
// Fog & Edge smart-building backend - Azure infrastructure as code.
//
//   Function App (Flex Consumption)  - autoscaling compute, scale-to-zero
//   Service Bus queue                - durable buffer + autoscaling signal
//   Cosmos DB (serverless)           - telemetry store, partitioned by /zone
//   Application Insights             - traces, live metrics, scale evidence
//   Storage account                  - required by the Functions runtime
//
// Deploy:  az deployment group create -g <rg> -f infra/main.bicep \
//            -p namePrefix=fogedge apiKey=<shared-secret>
// ---------------------------------------------------------------------------

@description('Prefix for all resource names (lowercase, 3-11 chars).')
@minLength(3)
@maxLength(11)
param namePrefix string = 'fogedge'

@description('Deployment location.')
param location string = resourceGroup().location

@description('Shared secret the fog nodes present in the X-Api-Key header.')
@secure()
param apiKey string

var suffix        = uniqueString(resourceGroup().id)
var storageName   = toLower('${namePrefix}st${substring(suffix, 0, 6)}')
var sbNamespace   = '${namePrefix}-sb-${substring(suffix, 0, 6)}'
var cosmosName    = '${namePrefix}-cosmos-${substring(suffix, 0, 6)}'
var functionName  = '${namePrefix}-func-${substring(suffix, 0, 6)}'
var planName      = '${namePrefix}-plan'
var aiName        = '${namePrefix}-ai'
var queueName     = 'telemetry'
var databaseName  = 'telemetry'
var containerName = 'readings'

// ---------------------------------------------------------------- storage
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
  }
}

// ------------------------------------------------------------ service bus
// Standard tier is required for the queue features the Functions scaler uses.
resource sb 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: sbNamespace
  location: location
  sku: { name: 'Standard', tier: 'Standard' }
}

resource sbQueue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: sb
  name: queueName
  properties: {
    // At-least-once delivery with a bounded retry budget; anything that fails
    // 10 times is a poison message and is moved to the dead-letter queue
    // rather than blocking the consumer group.
    maxDeliveryCount: 10
    lockDuration: 'PT1M'
    defaultMessageTimeToLive: 'P7D'
    deadLetteringOnMessageExpiration: true
    enablePartitioning: true
  }
}

resource sbAuth 'Microsoft.ServiceBus/namespaces/AuthorizationRules@2022-10-01-preview' existing = {
  parent: sb
  name: 'RootManageSharedAccessKey'
}

// ---------------------------------------------------------------- cosmos
// Serverless: billed per request unit, so an idle demo costs effectively zero
// while still scaling to thousands of RU/s during the load test.
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: cosmosName
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    capabilities: [ { name: 'EnableServerless' } ]
    consistencyPolicy: { defaultConsistencyLevel: 'Session' }
    locations: [ { locationName: location, failoverPriority: 0 } ]
  }
}

resource cosmosDb 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  parent: cosmos
  name: databaseName
  properties: { resource: { id: databaseName } }
}

resource cosmosContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: cosmosDb
  name: containerName
  properties: {
    resource: {
      id: containerName
      // /zone spreads writes across zones and keeps per-zone dashboard
      // queries single-partition. See report Sec. III-D.
      partitionKey: { paths: [ '/zone' ], kind: 'Hash' }
      defaultTtl: -1              // per-document TTL honoured (raw samples)
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [ { path: '/doc_type/?' }, { path: '/sensor_type/?' }
                       , { path: '/zone/?' }, { path: '/window_end/?' }
                       , { path: '/ts/?' } ]
        excludedPaths: [ { path: '/*' } ]   // index only what we query on
      }
    }
  }
}

// ------------------------------------------------------ app insights
resource ai 'Microsoft.Insights/components@2020-02-02' = {
  name: aiName
  location: location
  kind: 'web'
  properties: { Application_Type: 'web' }
}

// ------------------------------------------------------ function app
resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: planName
  location: location
  sku: { name: 'Y1', tier: 'Dynamic' }   // Consumption: event-driven autoscale
  properties: { reserved: true }         // Linux
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: functionName
  location: location
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Python|3.11'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      cors: { allowedOrigins: [ '*' ] }
      appSettings: [
        { name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME',    value: 'python' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: ai.properties.ConnectionString }
        { name: 'ServiceBusConnection', value: listKeys(sbAuth.id, sbAuth.apiVersion).primaryConnectionString }
        { name: 'CosmosConnection',     value: cosmos.listConnectionStrings().connectionStrings[0].connectionString }
        { name: 'COSMOS_DB',        value: databaseName }
        { name: 'COSMOS_CONTAINER', value: containerName }
        { name: 'FOG_API_KEY',      value: apiKey }
        // Cap the fan-out so a burst cannot exhaust the Cosmos RU budget.
        { name: 'AzureFunctionsJobHost__extensions__serviceBus__messageHandlerOptions__maxConcurrentCalls', value: '32' }
      ]
    }
  }
}

output functionAppName string = functionApp.name
output ingestUrl string = 'https://${functionApp.properties.defaultHostName}/api/ingest'
output dashboardUrl string = 'https://${functionApp.properties.defaultHostName}/api/dashboard'
output serviceBusQueue string = sbQueue.name
output cosmosAccount string = cosmos.name
