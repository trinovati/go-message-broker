package messagebroker

/*
Interface that defines the behavior of a semaphore that will controll concurent access to an asset.
*/
type SemaphoreManager interface {
	/*
		Use a permition sized to permissionsTaken, return a error in case of no permissions availble.
	*/
	GetPermission(permissionsTaken int64) (err error)

	/*
		Gives back permition sized as permissionsReleased to the semaphore.
	*/
	ReleasePermission(permissionsReleased int64)
}
