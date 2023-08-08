// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package librarylogging.actions;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import librarylogging.impl.MendixLog4jAppender;

/**
 * FOR DEVELOPMENT PURPOSES ONLY!
 * Logs everything from a certain level to a certain node.
 */
public class SetRootLevel extends CustomJavaAction<java.lang.Boolean>
{
	private final java.lang.String logNode;
	private final librarylogging.proxies.LogLevels logLevel;

	public SetRootLevel(
		IContext context,
		java.lang.String _logNode,
		java.lang.String _logLevel
	)
	{
		super(context);
		this.logNode = _logNode;
		this.logLevel = _logLevel == null ? null : librarylogging.proxies.LogLevels.valueOf(_logLevel);
	}

	@java.lang.Override
	public java.lang.Boolean executeAction() throws Exception
	{
		// BEGIN USER CODE
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		MendixLog4jAppender mendixAppender = new MendixLog4jAppender(logNode, null, logNode);
		mendixAppender.start();
		
		Level level = Level.OFF;
		switch(logLevel) {
		case DEBUG: level = Level.DEBUG; break;
		case ERROR: level = Level.ERROR; break;
		case FATAL: level = Level.FATAL; break;
		case INFO: level = Level.INFO; break;
		case OFF: level = Level.OFF; break;
		case TRACE: level = Level.TRACE; break;
		case WARN: level = Level.WARN; break;
		}
		
		config.getRootLogger().setLevel(level);
		config.getRootLogger().addAppender(mendixAppender, level, null);
		
		ctx.updateLoggers();
		return true;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 * @return a string representation of this action
	 */
	@java.lang.Override
	public java.lang.String toString()
	{
		return "SetRootLevel";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
