package builder

import (
	"context"
	"math"
	"os"
	"path"
	"strings"
	"time"
	"sync"
	"sync/atomic"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/environment"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	localBuildExecutorDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "local_build_executor_duration_seconds",
			Help:      "Amount of time spent per build execution step, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, math.Pow(10.0, 1.0/3.0), 6*3+1),
		},
		[]string{"step"})
	localBuildExecutorDurationSecondsPrepareFilesystem = localBuildExecutorDurationSeconds.WithLabelValues("PrepareFilesystem")
	localBuildExecutorDurationSecondsGetActionCommand  = localBuildExecutorDurationSeconds.WithLabelValues("GetActionCommand")
	localBuildExecutorDurationSecondsRunCommand        = localBuildExecutorDurationSeconds.WithLabelValues("RunCommand")
	localBuildExecutorDurationSecondsUploadOutput      = localBuildExecutorDurationSeconds.WithLabelValues("UploadOutput")
	MAX_CONCURRENT_DOWNLOADS                           = 128
	MAX_DOWNLOAD_FILE_RETRIES                          = 3
)

func init() {
	prometheus.MustRegister(localBuildExecutorDurationSeconds)
}

type localBuildExecutor struct {
	contentAddressableStorage cas.ContentAddressableStorage
	environmentManager        environment.Manager
	workChan                  chan func()
}

type workspaceBuilder struct {
	ctx context.Context
	contentAddressableStorage cas.ContentAddressableStorage
	errorCounter uint32
	errorMsg atomic.Value // Always error type.
	workChan chan func()
}

func NewWorkspaceBuilder(ctx context.Context, contentAddressableStorage cas.ContentAddressableStorage, workChan chan func()) *workspaceBuilder {
	return &workspaceBuilder{
		ctx: ctx,
		contentAddressableStorage: contentAddressableStorage,
		errorCounter: 0,
		workChan: workChan,
	}
}

func (wb *workspaceBuilder) GetError() error {
	value := wb.errorMsg.Load()
	if value == nil {
		return nil
	}
	return value.(error)
}

func (wb *workspaceBuilder) setError(msg error) {
	// Flag the builder to abort all other downloads and store the error result.
	atomic.AddUint32(&wb.errorCounter, 1)
	// Multiple errors may have happened here, but we only care about one of them (any of them).
	wb.errorMsg.Store(msg)
}

func (wb *workspaceBuilder) enqueueWork(wg *sync.WaitGroup, work func(), debugName string) {
	if (atomic.LoadUint32(&wb.errorCounter) != 0) {
		// Looks like another download failed, so we don't want to continue any additional work.
		return
	}
	wg.Add(1);
	wb.workChan <-func() {
		defer func() {
			wg.Done()
		}()
		if (atomic.LoadUint32(&wb.errorCounter) != 0) {
			// Looks like another download failed, so we don't want to continue any additional work.
			return
		}
		work()
	}
}

func (wb *workspaceBuilder) EnqueueGetDirectory(partialDigest *remoteexecution.Digest, parentDigest *util.Digest,
                                                inputDirectory filesystem.Directory,
                                                components []string) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	// Make a copy of the slice because we might be mutating it later.
	wb.enqueueWork(&wg, func() {
		// Obtain directory.
		digest, err := parentDigest.NewDerivedDigest(partialDigest)
		if err != nil {
			wb.setError(util.StatusWrapf(err, "Failed to extract digest for input directory %#v", path.Join(components...)))
			return
		}
		directory, err := wb.contentAddressableStorage.GetDirectory(wb.ctx, digest)
		if err != nil {
			wb.setError(util.StatusWrapf(err, "Failed to obtain input directory %#v", path.Join(components...)))
			return
		}

		// Create children.
		for _, file := range directory.Files {
			childComponents := append([]string{}, components...)  // Ensure we have a full copy because of our lambda.
			childComponents = append(childComponents, file.Name)
			childDigest, err := digest.NewDerivedDigest(file.Digest)
			if err != nil {
				wb.setError(util.StatusWrapf(err, "Failed to extract digest for input file %#v", path.Join(childComponents...)))
				return
			}
			fileCopy := file  // `file` is a reference and will be replaced on every iteration.
			wb.enqueueWork(&wg, func() {
				var err error
				for i := 0; i < MAX_DOWNLOAD_FILE_RETRIES; i++ {
					if err = wb.contentAddressableStorage.GetFile(wb.ctx, childDigest, inputDirectory, fileCopy.Name, fileCopy.IsExecutable); err == nil {
						return  // Success path.
					}
				}
				wb.setError(util.StatusWrapf(err, "Failed to obtain input file %#v", path.Join(childComponents...)))
			}, path.Join(childComponents...))
		}
		for _, directory := range directory.Directories {
			childComponents := append([]string{}, components...)  // Ensure we have a full copy because of our lambda.
			childComponents = append(childComponents, directory.Name)
			if err := inputDirectory.Mkdir(directory.Name, 0777); err != nil {
				wb.setError(util.StatusWrapf(err, "Failed to create input directory %#v", path.Join(childComponents...)))
				return
			}
			childDirectory, err := inputDirectory.Enter(directory.Name)
			if err != nil {
				wb.setError(util.StatusWrapf(err, "Failed to enter input directory %#v", path.Join(childComponents...)))
				return
			}
			wg.Add(1)  // Ensure parent directory will not close until our children are closed.
			childWorkGroup := wb.EnqueueGetDirectory(directory.Digest, digest, childDirectory, childComponents)
			go func() {
				childWorkGroup.Wait()
				childDirectory.Close()
				wg.Done()
			}()
		}
		for _, symlink := range directory.Symlinks {
			// Gotcha: Symlinks are created, but the referenced objects may not be downloaded yet.
			if err := inputDirectory.Symlink(symlink.Target, symlink.Name); err != nil {
				childComponents := append(components, symlink.Name)
				wb.setError(util.StatusWrapf(err, "Failed to create input symlink %#v", path.Join(childComponents...)))
				return
			}
		}
	}, path.Join(components...))
	return &wg
}

// NewLocalBuildExecutor returns a BuildExecutor that executes build
// steps on the local system.
func NewLocalBuildExecutor(contentAddressableStorage cas.ContentAddressableStorage,
                           environmentManager environment.Manager) BuildExecutor {
	workChan := make(chan func(), math.MaxInt32)
	for i := 0;  i < MAX_CONCURRENT_DOWNLOADS; i++ {
		go func () {
			// This channel will contain all the download tasks, so we spin up MAX_CONCURRENT_DOWNLOADS go routines to
			// read from the channel and process the tasks.  This will block the goroutine until work is available if
			// no tasks are in the queue.
			for {
				download_task := <-workChan
				download_task()
			}
		}()
	}
	return &localBuildExecutor{
		contentAddressableStorage: contentAddressableStorage,
		environmentManager:        environmentManager,
		workChan:                  workChan,
	}
}

func (be *localBuildExecutor) uploadDirectory(ctx context.Context, outputDirectory filesystem.Directory,
																							parentDigest *util.Digest, children map[string]*remoteexecution.Directory,
																							components []string) (*remoteexecution.Directory, error) {
	files, err := outputDirectory.ReadDir()
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to read output directory %#v", path.Join(components...))
	}

	var directory remoteexecution.Directory
	for _, file := range files {
		name := file.Name()
		childComponents := append(components, name)
		switch fileType := file.Type(); fileType {
		case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
			digest, err := be.contentAddressableStorage.PutFile(ctx, outputDirectory, name, parentDigest)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to store output file %#v", path.Join(childComponents...))
			}
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       digest.GetPartialDigest(),
				IsExecutable: fileType == filesystem.FileTypeExecutableFile,
			})
		case filesystem.FileTypeDirectory:
			childDirectory, err := outputDirectory.Enter(name)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to enter output directory %#v", path.Join(childComponents...))
			}
			child, err := be.uploadDirectory(ctx, childDirectory, parentDigest, children, childComponents)
			childDirectory.Close()
			if err != nil {
				return nil, err
			}

			// Compute digest of the child directory. This requires serializing it.
			data, err := proto.Marshal(child)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to marshal output directory %#v", path.Join(childComponents...))
			}
			digestGenerator := parentDigest.NewDigestGenerator()
			if _, err := digestGenerator.Write(data); err != nil {
				return nil, util.StatusWrapf(err, "Failed to compute digest of output directory %#v",
																		 path.Join(childComponents...))
			}
			digest := digestGenerator.Sum()

			children[digest.GetKey(util.DigestKeyWithoutInstance)] = child
			directory.Directories = append(directory.Directories, &remoteexecution.DirectoryNode{
				Name:   name,
				Digest: digest.GetPartialDigest(),
			})
		case filesystem.FileTypeSymlink:
			target, err := outputDirectory.Readlink(name)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to read output symlink %#v", path.Join(childComponents...))
			}
			directory.Symlinks = append(directory.Symlinks, &remoteexecution.SymlinkNode{
				Name:   name,
				Target: target,
			})
		default:
			return nil, status.Errorf(codes.Internal, "Output file %#v is not a regular file, directory or symlink", name)
		}
	}
	return &directory, nil
}

func (be *localBuildExecutor) uploadTree(ctx context.Context, outputDirectory filesystem.Directory,
                                         parentDigest *util.Digest, components []string) (*util.Digest, error) {
	// Gather all individual directory objects and turn them into a tree.
	children := map[string]*remoteexecution.Directory{}
	root, err := be.uploadDirectory(ctx, outputDirectory, parentDigest, children, components)
	if err != nil {
		return nil, err
	}
	tree := &remoteexecution.Tree{
		Root: root,
	}
	for _, child := range children {
		tree.Children = append(tree.Children, child)
	}
	digest, err := be.contentAddressableStorage.PutTree(ctx, tree, parentDigest)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to store output directory %#v", path.Join(components...))
	}
	return digest, err
}

func (be *localBuildExecutor) createOutputParentDirectory(buildDirectory filesystem.Directory, outputParentPath string) (filesystem.Directory, error) {
	// Create and enter successive components, closing the former.
	components := strings.FieldsFunc(outputParentPath, func(r rune) bool { return r == '/' })
	d := buildDirectory
	for n, component := range components {
		if component != "." {
			if err := d.Mkdir(component, 0777); err != nil && !os.IsExist(err) {
				if d != buildDirectory {
					d.Close()
				}
				return nil, util.StatusWrapf(err, "Failed to create output directory %#v", path.Join(components[:n+1]...))
			}
			d2, err := d.Enter(component)
			if d != buildDirectory {
				d.Close()
			}
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to enter output directory %#v", path.Join(components[:n+1]...))
			}
			d = d2
		}
	}
	return d, nil
}

func (be *localBuildExecutor) Execute(ctx context.Context, request *remoteexecution.ExecuteRequest) (*remoteexecution.ExecuteResponse, bool) {
	timeStart := time.Now()

	// Fetch action and command.
	actionDigest, err := util.NewDigest(request.InstanceName, request.ActionDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to extract digest for action")), false
	}
	action, err := be.contentAddressableStorage.GetAction(ctx, actionDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to obtain action")), false
	}
	commandDigest, err := actionDigest.NewDerivedDigest(action.CommandDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to extract digest for command")), false
	}
	command, err := be.contentAddressableStorage.GetCommand(ctx, commandDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to obtain command")), false
	}
	timeAfterGetActionCommand := time.Now()
	localBuildExecutorDurationSecondsGetActionCommand.Observe(
		timeAfterGetActionCommand.Sub(timeStart).Seconds())

	// Obtain build environment.
	platformProperties := map[string]string{}
	if command.Platform != nil {
		for _, platformProperty := range command.Platform.Properties {
			platformProperties[platformProperty.Name] = platformProperty.Value
		}
	}
	environment, err := be.environmentManager.Acquire(actionDigest, platformProperties)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to acquire build environment")), false
	}
	defer environment.Release()

	// Set up inputs.
	buildDirectory := environment.GetBuildDirectory()
	workspaceBuilder := NewWorkspaceBuilder(ctx, be.contentAddressableStorage, be.workChan)
	wg := workspaceBuilder.EnqueueGetDirectory(action.InputRootDigest, actionDigest, buildDirectory, []string{"."})
	wg.Wait()
	fmt.Println("Done and ready to execute")
	if err := workspaceBuilder.GetError(); err != nil {
		return convertErrorToExecuteResponse(err), false
	}

	// Create and open parent directories of where we expect to see output.
	// Build rules generally expect the parent directories to already be
	// there. We later use the directory handles to extract output files.
	outputParentDirectories := map[string]filesystem.Directory{}
	for _, outputDirectory := range command.OutputDirectories {
		dirPath := path.Dir(outputDirectory)
		if _, ok := outputParentDirectories[dirPath]; !ok {
			dir, err := be.createOutputParentDirectory(buildDirectory, dirPath)
			if err != nil {
				return convertErrorToExecuteResponse(err), false
			}
			outputParentDirectories[dirPath] = dir
			if dir != buildDirectory {
				defer dir.Close()
			}
		}
	}
	for _, outputFile := range command.OutputFiles {
		dirPath := path.Dir(outputFile)
		if _, ok := outputParentDirectories[dirPath]; !ok {
			dir, err := be.createOutputParentDirectory(buildDirectory, dirPath)
			if err != nil {
				return convertErrorToExecuteResponse(err), false
			}
			outputParentDirectories[dirPath] = dir
			if dir != buildDirectory {
				defer dir.Close()
			}
		}
	}

	timeAfterPrepareFilesytem := time.Now()
	localBuildExecutorDurationSecondsPrepareFilesystem.Observe(
		timeAfterPrepareFilesytem.Sub(timeAfterGetActionCommand).Seconds())

	// Invoke command.
	environmentVariables := map[string]string{}
	for _, environmentVariable := range command.EnvironmentVariables {
		environmentVariables[environmentVariable.Name] = environmentVariable.Value
	}
	runResponse, err := environment.Run(ctx, &runner.RunRequest{
		Arguments:            command.Arguments,
		EnvironmentVariables: environmentVariables,
		WorkingDirectory:     command.WorkingDirectory,
		StdoutPath:           ".stdout.txt",
		StderrPath:           ".stderr.txt",
	})
	if err != nil {
		return convertErrorToExecuteResponse(err), false
	}
	timeAfterRunCommand := time.Now()
	localBuildExecutorDurationSecondsRunCommand.Observe(
		timeAfterRunCommand.Sub(timeAfterPrepareFilesytem).Seconds())

	response := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode: runResponse.ExitCode,
		},
	}

	// Upload command output. In the common case, the files are
	// empty. If that's the case, don't bother setting the digest to
	// keep the ActionResult small.
	stdoutDigest, err := be.contentAddressableStorage.PutFile(ctx, buildDirectory, ".stdout.txt", actionDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to store stdout")), false
	}
	if stdoutDigest.GetSizeBytes() > 0 {
		response.Result.StdoutDigest = stdoutDigest.GetPartialDigest()
	}
	stderrDigest, err := be.contentAddressableStorage.PutFile(ctx, buildDirectory, ".stderr.txt", actionDigest)
	if err != nil {
		return convertErrorToExecuteResponse(util.StatusWrap(err, "Failed to store stderr")), false
	}
	if stderrDigest.GetSizeBytes() > 0 {
		response.Result.StderrDigest = stderrDigest.GetPartialDigest()
	}

	// Upload output files.
	for _, outputFile := range command.OutputFiles {
		outputParentDirectory := outputParentDirectories[path.Dir(outputFile)]
		outputBaseName := path.Base(outputFile)
		fileInfo, err := outputParentDirectory.Lstat(outputBaseName)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to read attributes of output file %#v", outputFile)), false
		}
		switch fileType := fileInfo.Type(); fileType {
		case filesystem.FileTypeRegularFile, filesystem.FileTypeExecutableFile:
			digest, err := be.contentAddressableStorage.PutFile(ctx, outputParentDirectory, outputBaseName, actionDigest)
			if err != nil {
				return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to store output file %#v", outputFile)), false
			}
			response.Result.OutputFiles = append(response.Result.OutputFiles, &remoteexecution.OutputFile{
				Path:         outputFile,
				Digest:       digest.GetPartialDigest(),
				IsExecutable: fileType == filesystem.FileTypeExecutableFile,
			})
		case filesystem.FileTypeSymlink:
			target, err := outputParentDirectory.Readlink(outputBaseName)
			if err != nil {
				return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to read output symlink %#v", outputFile)), false
			}
			response.Result.OutputFileSymlinks = append(response.Result.OutputFileSymlinks, &remoteexecution.OutputSymlink{
				Path:   outputFile,
				Target: target,
			})
		default:
			return convertErrorToExecuteResponse(status.Errorf(codes.Internal, "Output file %#v is not a regular file or symlink", outputFile)), false
		}
	}

	// Upload output directories.
	for _, outputDirectory := range command.OutputDirectories {
		outputParentDirectory := outputParentDirectories[path.Dir(outputDirectory)]
		outputBaseName := path.Base(outputDirectory)
		fileInfo, err := outputParentDirectory.Lstat(outputBaseName)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to read attributes of output directory %#v", outputDirectory)), false
		}
		switch fileInfo.Type() {
		case filesystem.FileTypeDirectory:
			directory, err := outputParentDirectory.Enter(outputBaseName)
			if err != nil {
				return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to enter output directory %#v", outputDirectory)), false
			}
			digest, err := be.uploadTree(ctx, directory, actionDigest, []string{outputDirectory})
			directory.Close()
			if err != nil {
				return convertErrorToExecuteResponse(err), false
			}
			if digest != nil {
				response.Result.OutputDirectories = append(response.Result.OutputDirectories, &remoteexecution.OutputDirectory{
					Path:       outputDirectory,
					TreeDigest: digest.GetPartialDigest(),
				})
			}
		case filesystem.FileTypeSymlink:
			target, err := outputParentDirectory.Readlink(outputBaseName)
			if err != nil {
				return convertErrorToExecuteResponse(util.StatusWrapf(err, "Failed to read output symlink %#v", outputDirectory)), false
			}
			response.Result.OutputDirectorySymlinks = append(response.Result.OutputDirectorySymlinks, &remoteexecution.OutputSymlink{
				Path:   outputDirectory,
				Target: target,
			})
		default:
			return convertErrorToExecuteResponse(status.Errorf(codes.Internal, "Output file %#v is not a directory or symlink", outputDirectory)), false
		}
	}

	timeAfterUpload := time.Now()
	localBuildExecutorDurationSecondsUploadOutput.Observe(
		timeAfterUpload.Sub(timeAfterRunCommand).Seconds())

	return response, !action.DoNotCache && response.Result.ExitCode == 0
}
