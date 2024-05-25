use std::collections::BTreeMap;

use clap::{Parser, Subcommand};
use colored::*;
use k8s_crds_longhorn::{RecurringJob, RecurringJobSpec, RecurringJobTask, Volume};
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::Config;
use serde_json::json;

use kube::config::KubeConfigOptions;
use kube::{
    api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams, ResourceExt},
    Client,
};

fn get_job_string(job: &RecurringJobTask) -> String {
    match job {
        RecurringJobTask::Snapshot => "snapshot".to_string(),
        RecurringJobTask::Backup => "backup".to_string(),
        _ => "Other".to_string(),
    }
}

/// A command line tool to for managing harvester volumes
/// Please set the SOURCE_CONTEXT and TARGET_CONTEXT to choose clusters
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[clap(long, short, action)]
    verbose: bool,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List all current recurring jobs.
    Jobs,

    /// List all the PVCs and their associated volumes
    Pvcs,

    /// Add a backup task for a given pvc
    Backup {
        /// The pvc to update
        pvc: String,
        /// How man backups to keep
        retention: Option<i64>,
    },

    /// Add a snapshot task for a given pvc
    Snapshot {
        /// The pvc to update
        pvc: String,
        /// The number of  snapshots to keep
        retention: Option<i64>,
    },
}

struct Context {
    client: Client,
}

impl From<&'static str> for Context {
    #[tokio::main]
    async fn from(config: &'static str) -> Self {
        Context::new(config).await
    }
}

/// Used to create clients from the config file using the name of the context
impl Context {
    async fn new(config: &str) -> Context {
        let config = kube::Config::from_kubeconfig(&KubeConfigOptions {
            context: Some(config.to_string()),
            ..Default::default()
        })
        .await
        .unwrap(); // or .expect("Unable to load kubeconfig"    }

        Context {
            client: Client::try_from(config).unwrap(),
        } // or .expect("Unable to create client")
    }

    async fn get_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let volumes: Api<PersistentVolumeClaim> = Api::all(self.client.clone());
        volumes.list(&ListParams::default()).await.unwrap().items
    }
}

/// Helper function to get the harvester client from context
async fn get_harvester_client() -> Client {
    let env_harvester = "TARGET_CONTEXT";
    let context = std::env::var(env_harvester).unwrap_or("harvester".to_string()); // or .expect("Unable to get HARVESTER_CONTEXT");
    Context::new(context.as_str()).await.client
}

/// Creates the recurring job in harvester for backups or snapshots
async fn create_recurring_job(
    name: &str,
    cron: &str,
    task: RecurringJobTask,
    retention: i64,
) -> String {
    let client = get_harvester_client().await;
    let jobs: Api<RecurringJob> = Api::namespaced(client, "longhorn-system");
    let mut new_job = RecurringJob::default();
    let job_name = format!("{}-{}", name, get_job_string(&task));
    new_job.metadata.name = Some(job_name.clone());
    new_job.spec = RecurringJobSpec {
        task: Some(task),
        cron: Some(cron.to_string()),
        concurrency: Some(1),
        retain: Some(retention),
        labels: Some(BTreeMap::from([("app".to_string(), name.to_string())])),
        groups: Some(vec![]),
        name: Some(job_name.clone()),
    };

    println!("{}", serde_yml::to_string(&new_job).unwrap());
    let _ = jobs
        .delete(job_name.as_str(), &DeleteParams::default())
        .await;

    jobs.create(&PostParams::default(), &new_job).await.unwrap();

    job_name.to_string()
}

/// Helper function for the client to create the recurring job and then label the volume with the job name.
async fn call_job(pvc: String, retain: i64, task: RecurringJobTask) {
    let name = create_recurring_job(&pvc, "0 0 * * *", task, retain).await;
    label_pvc(&pvc, &name).await;

    println!("Created a recurring job")
}

/// Adds a label to the corresponding volume for your pvc so that it can be  used by the recurring job.
async fn label_pvc(pvc: &str, job: &str) {
    // patch the volume to add the recurring job
    let volumes: Api<k8s_crds_longhorn::Volume> =
        Api::namespaced(get_harvester_client().await, "longhorn-system");

    let volume = get_volume(pvc).await;

    let mut volume_labels = volumes
        .get(volume.as_str())
        .await
        .unwrap()
        .metadata
        .labels
        .unwrap();

    let job_label = format!("recurring-job.longhorn.io/{}", job);
    volume_labels.insert(job_label, "enabled".to_string());

    let patch = json!(
    {
        "apiVersion": "longhorn.io/v1beta2",
        "kind": "Volume",
        "metadata": {
            "labels":  volume_labels}
    });

    volumes
        .patch(
            volume.as_str(),
            &PatchParams::apply("kube"),
            &Patch::Apply(&patch),
        )
        .await
        .unwrap();
}

/// Associates the pvcs on the source cluster with the volumes on the target cluster and returns them
async fn get_linked_pvcs() -> Vec<LinkedPVC> {
    let target_pvcs = Context::new("harvester").await.get_pvcs().await;
    let volumes: Api<Volume> = Api::namespaced(get_harvester_client().await, "longhorn-system");

    let volumes = volumes.list(&ListParams::default()).await.unwrap().items;
    let env_source = std::env::var("SOURCE_CONTEXT").unwrap_or("production".to_string());
    let env_target = std::env::var("TARGET_CONTEXT").unwrap_or("harvester".to_string());

    let source_pvcs = Context::new(env_source.as_str()).await.get_pvcs().await;

    source_pvcs
        .into_iter()
        .map(|source| {
            let volume;
            if env_source != env_target {
                volume = target_pvcs
                    .iter()
                    .find(|target| {
                        target.name_any() == source.clone().spec.unwrap().volume_name.unwrap()
                    })
                    .expect("No matching target volume was found for the source pvc")
                    .to_owned()
                    .spec
                    .clone()
                    .unwrap()
                    .volume_name
                    .unwrap();
            } else {
                volume = source.spec.clone().unwrap().volume_name.unwrap();
            }

            let jobs = volumes
                .iter()
                .find(|vol| vol.name_any() == volume)
                .unwrap()
                .labels()
                .keys()
                .filter(|key| key.starts_with("recurring-job"))
                .map(|key| key.split('/').last().unwrap().to_string())
                .filter(|key| key != "default")
                .collect();

            LinkedPVC {
                pvc: source.name_any(),
                volume,
                jobs,
            }
        })
        .collect()
}

/// Gets the volume associated with the get linked_pvcs. Used to grab volumes when labeling
async fn get_volume(pvc: &str) -> String {
    get_linked_pvcs()
        .await
        .into_iter()
        .find(|linked_pvc| linked_pvc.pvc == pvc)
        .unwrap()
        .volume
}

/// Helper function for clap to print all recurring jobs and their info
async fn print_recurring_jobs() {
    println!("Retrieving Jobs:");
    let recurring_jobs: Api<RecurringJob> = Api::all(get_harvester_client().await);

    recurring_jobs
        .list(&ListParams::default())
        .await
        .unwrap()
        .into_iter()
        .for_each(|job| {
            println!(
                "Job: {}, {} {} {}",
                job.metadata.name.unwrap().red(),
                job.spec.labels.unwrap().values().next().unwrap().green(),
                job.spec.cron.unwrap().blue(),
                match job.spec.task.unwrap() {
                    RecurringJobTask::Backup => "Backup".to_string(),
                    _ => "Snapshot".to_string(),
                }
            )
        })
}

/// Helper function for clap to print all pvcs and their info
async fn print_pvcs() {
    get_linked_pvcs().await.into_iter().for_each(|f| {
        println!(
            "{}\n\tVolume: {}\n\tJobs: {}",
            f.pvc.green(),
            f.volume.red(),
            f.jobs.join(" ").blue()
        );
    })
}

/// Structure to hold relationship between pvc on the source cluster and the volume and jobs on the harvester cluster
#[derive(Debug)]
struct LinkedPVC {
    pvc: String,
    volume: String,
    jobs: Vec<String>,
}

#[tokio::main]
async fn main() {
    let cli: Cli = Cli::parse();

    match cli.command {
        Command::Jobs => {
            print_recurring_jobs().await;
        }
        Command::Pvcs => {
            print_pvcs().await;
        }
        Command::Backup { pvc, retention } => {
            call_job(pvc, retention.unwrap_or(2), RecurringJobTask::Backup).await;
        }
        Command::Snapshot { pvc, retention } => {
            call_job(pvc, retention.unwrap_or(4), RecurringJobTask::Snapshot).await;
        }
    }
}
