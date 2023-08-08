// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package kafka.actions;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.webui.CustomJavaAction;

public class WatchObjectForChange extends CustomJavaAction<java.lang.Void>
{
	private final IMendixObject objectToWatch;
	private final java.lang.String onChanged;

	public WatchObjectForChange(
		IContext context,
		IMendixObject _objectToWatch,
		java.lang.String _onChanged
	)
	{
		super(context);
		this.objectToWatch = _objectToWatch;
		this.onChanged = _onChanged;
	}

	@java.lang.Override
	public java.lang.Void executeAction() throws Exception
	{
		// BEGIN USER CODE
		CheckForChangesThread thread = new CheckForChangesThread(objectToWatch, onChanged);
		changesThreads.add(thread);
		thread.start();
		return null;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 * @return a string representation of this action
	 */
	@java.lang.Override
	public java.lang.String toString()
	{
		return "WatchObjectForChange";
	}

	// BEGIN EXTRA CODE
	private static ILogNode LOGGER = Core.getLogger("WatchObject");
	public static List<CheckForChangesThread> changesThreads = new LinkedList<>();
	
	class CheckForChangesThread extends Thread {
		private IMendixIdentifier objectId;
		private String onChangedMicroflow;
		private Date lastChangedDate;
		private boolean running = true;
		
		public CheckForChangesThread(IMendixObject objectToWatch, String onChangedMicroflow) throws CoreException {
			this.objectId = objectToWatch.getId();
			lastChangedDate = objectToWatch.getChangedDate(getContext());
			this.onChangedMicroflow = onChangedMicroflow;
		}
		
		public void run() {
			while(running) {
				try {
					IContext context = Core.createSystemContext();
					IMendixObject freshVersion = Core.retrieveId(context, objectId);
					if (!freshVersion.getChangedDate(context).equals(lastChangedDate)) {
						lastChangedDate = freshVersion.getChangedDate(context);
						LOGGER.debug("Object " + objectId.toLong() + " changed.");
						Core.microflowCall(onChangedMicroflow).execute(context);
					}
				} catch (CoreException e) {
					LOGGER.error("Error while checking for changed object.", e);
				}
				try {
					sleep(2500);
				} catch (InterruptedException e) {}
			}
		}
		
		public void disable() {
			this.running = false;
		}
	}
	// END EXTRA CODE
}
