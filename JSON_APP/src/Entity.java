import java.util.concurrent.Callable;

public abstract class Entity  {
    public String entityName;
    LoadStatus loadStatus = LoadStatus.ENQUEUED;
    void startLoad() {
        if (loadStatus != LoadStatus.ENQUEUED) {
            throw new RuntimeException();
        }
        loadStatus = LoadStatus.RUNNING;
    }
    void finishLoad() {
        loadStatus = LoadStatus.SUCCEEDED;
    }
    public LoadStatus getStatus() {
        return loadStatus;
    }
    @Override
    public boolean equals(Object obj) {
        boolean lReturn = false;
        if (obj instanceof Entity) {
            lReturn = ((Entity) obj).entityName.equals(this.entityName);
        }
        return lReturn;
    }
//    private boolean executionFinished = false;

/*    synchronized void setExecutionFinished() {
        executionFinished = true;
    }

    synchronized boolean getExecutionFinished() {
        return executionFinished;
    }*/

    public Entity(String entityName) {
        this.entityName = entityName;
    }

    public abstract void entityLoad();
}

class StgIndexSecurityWeight extends Entity {
    public StgIndexSecurityWeight() {
        super("StgIndexSecurityWeight");
    }
    public synchronized void entityLoad() {

        startLoad();
        System.out.println("Start Load StgIndexSecurityWeight! " + loadStatus);
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load StgIndexSecurityWeight! " + loadStatus);
    }
}

class OdsIndexSecurityWeight extends Entity {
    public OdsIndexSecurityWeight() {
        super("OdsIndexSecurityWeight");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load OdsIndexSecurityWeight! " + loadStatus);
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load OdsIndexSecurityWeight! " + loadStatus);
    }
}

class DdsIndexHub extends Entity {
    public DdsIndexHub() {
        super("DdsIndexHub");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsIndexHub! " + loadStatus);
        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsIndexHub! " + loadStatus);
    }
}

class DdsIndexSat extends Entity {
    public DdsIndexSat() {
        super("DdsIndexSat");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsIndexSat! " + loadStatus);
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsIndexSat! " + loadStatus);
    }
}

class StgSecurityRate extends Entity {
    public StgSecurityRate() {
        super("StgSecurityRate");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load StgSecurityRate! " + loadStatus);
        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load StgSecurityRate! " + loadStatus);
    }
}

class OdsSecurityRate extends Entity {
    public OdsSecurityRate() {
        super("OdsSecurityRate");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load OdsSecurityRate! " + loadStatus);
        try {
            Thread.sleep(7_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load OdsSecurityRate! " + loadStatus);
    }
}

class DdsSecurityHub extends Entity {
    public DdsSecurityHub() {
        super("DdsSecurityHub");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsSecurityHub! " + loadStatus);
        try {
            Thread.sleep(6_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsSecurityHub! " + loadStatus);
    }
}

class DdsSecuritySat extends Entity {
    public DdsSecuritySat() {
        super("DdsSecuritySat");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsSecuritySat! " + loadStatus);
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsSecuritySat! " + loadStatus);
    }
}

class DdsIndexSecurityLink extends Entity {
    public DdsIndexSecurityLink() {
        super("DdsIndexSecurityLink");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsIndexSecurityLink! " + loadStatus);
        try {
            Thread.sleep(8_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsIndexSecurityLink! " + loadStatus);
    }
}


class DdsIndexSecuritySat extends Entity {
    public DdsIndexSecuritySat() {
        super("DdsIndexSecuritySat");
    }
    public synchronized void entityLoad() {
        startLoad();
        System.out.println("Start Load DdsIndexSecuritySat! " + loadStatus);
        try {
            Thread.sleep(6_000);
        } catch (InterruptedException e)
        {}
        finishLoad();
        System.out.println("Finish Load DdsIndexSecuritySat! " + loadStatus);
    }
}