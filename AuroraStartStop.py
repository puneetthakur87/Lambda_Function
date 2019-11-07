import boto3
import traceback

rds = boto3.client('rds')

def lambda_handler(event, context):

    try:
       
        aurora_db_start_stop(event, context)
       
        return "Successfully Done!!"
       
    except Exception as e:
            displayException(e)
            traceback.print_exc()


def aurora_db_start_stop(event, context):

    action = event.get('action')

    if action is None:
        action = ''

    if action.lower() not in ['start', 'stop']:
        print ("No action taken. aurora_db_start_stop() aborted.")
    else:
        clusters_rds = rds.describe_db_clusters().get('DBClusters', [])
       
        for cluster_rds in clusters_rds:
            

            try:
                if 'aurora' in cluster_rds['Engine']:
                   
                    tags = rds.list_tags_for_resource(ResourceName = cluster_rds['DBClusterArn']).get('TagList',[])

                    for tag in tags:
           
                        if tag['Key'] == 'Epsagon-Aurora-Tag':
                           
                            clusterState = cluster_rds['Status']

                            # Start or stop instance
                            if clusterState == 'available' and action == 'stop':

                                print ("We got this Epsagon Aurora DB cluster " + cluster_rds['DBClusterIdentifier'] + " and can be stopped")
                               
                                rds.stop_db_cluster(
                                    DBClusterIdentifier = cluster_rds['DBClusterIdentifier']
                                )
                                print ("Epsagon Aurora DB cluster has been %s stopped" % cluster_rds['DBClusterIdentifier'])
                               
                       
                            elif clusterState == 'stopped' and action == 'start':
                                   
                                print ("We got this Epsagon Aurora DB cluster " + cluster_rds['DBClusterIdentifier'] + " and can be started")
                               
                                rds.start_db_cluster(
                                    DBClusterIdentifier = cluster_rds['DBClusterIdentifier']
                                )
                                print ("Epsagon Aurora DB cluster has been %s started" % cluster_rds['DBClusterIdentifier'])
                               
                            else:
                                print ("Epsagon Aurora DB cluster status %s is %s. It is not a right status for starting or stopping." % (cluster_rds['DBClusterIdentifier'], instanceState))
                           
            except Exception as e:
                displayException(e)
           

def displayException(exception):
    exception_type = exception.__class__.__name__
    exception_message = str(exception)

    print("Exception type: %s; Exception message: %s;" % (exception_type, exception_message))
